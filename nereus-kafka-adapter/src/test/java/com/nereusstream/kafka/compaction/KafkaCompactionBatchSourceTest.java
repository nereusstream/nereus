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

package com.nereusstream.kafka.compaction;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadSourceRef;
import com.nereusstream.api.StreamId;
import com.nereusstream.materialization.ExactSourceRangeReader;
import com.nereusstream.materialization.ExactSourceRead;
import com.nereusstream.materialization.ExactSourceReadSummary;
import com.nereusstream.materialization.SourceGeneration;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class KafkaCompactionBatchSourceTest {
  @Test
  void bindsTheExactReaderToTheRecoveredPlanStream() {
    KafkaCompactionPlan plan = KafkaCompactionPlanCodecV1Test.fixture("UNCOMPRESSED").plan();
    AtomicReference<StreamId> requestedStream = new AtomicReference<>();
    KafkaCompactionBatchSource source =
        new KafkaCompactionBatchSource(
            streamId -> {
              requestedStream.set(streamId);
              return (generation, options) ->
                  CompletableFuture.completedFuture(exactRead(generation, new AtomicInteger()));
            },
            options(),
            Runnable::run);

    try (KafkaCompactionBatchSource.PassStreams ignored = source.open(plan)) {
      assertThat(requestedStream).hasValue(plan.streamId());
    }
  }

  @Test
  void opensIndependentDecisionAndOutputStreamsFromTheRecoveredPlan() {
    KafkaCompactionPlan plan = KafkaCompactionPlanCodecV1Test.fixture("UNCOMPRESSED").plan();
    AtomicInteger closes = new AtomicInteger();
    ExactSourceRangeReader reader =
        (source, options) -> CompletableFuture.completedFuture(exactRead(source, closes));
    KafkaCompactionBatchSource source =
        new KafkaCompactionBatchSource(reader, options(), Runnable::run);

    try (KafkaCompactionBatchSource.PassStreams passes = source.open(plan)) {
      assertThat(collect(passes.decisionHorizon()).join())
          .extracting(ReadBatch::range)
          .containsExactly(new OffsetRange(0, 2), new OffsetRange(2, 3));
      assertThat(collect(passes.outputCoverage()).join())
          .extracting(ReadBatch::range)
          .containsExactly(new OffsetRange(0, 2));
      assertThat(closes).hasValue(3);
    }
  }

  private static ExactSourceRead exactRead(SourceGeneration source, AtomicInteger closes) {
    ReadBatch batch =
        new ReadBatch(
            source.range(),
            source.payloadFormat(),
            new byte[Math.toIntExact(source.logicalBytes())],
            source.schemaRefs(),
            source.projectionRef(),
            new ReadSourceRef(
                source.range(),
                source.generation(),
                source.commitVersion(),
                source.readTarget(),
                source.targetIdentitySha256()));
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
        return CompletableFuture.completedFuture(
            new ExactSourceReadSummary(
                source.range(),
                source.recordCount(),
                source.entryCount(),
                source.logicalBytes(),
                new Checksum(ChecksumType.SHA256, "e".repeat(64))));
      }

      @Override
      public void close() {
        closes.incrementAndGet();
      }
    };
  }

  private static CompletableFuture<List<ReadBatch>> collect(Flow.Publisher<ReadBatch> publisher) {
    CompletableFuture<List<ReadBatch>> completion = new CompletableFuture<>();
    ArrayList<ReadBatch> batches = new ArrayList<>();
    publisher.subscribe(
        new Flow.Subscriber<>() {
          @Override
          public void onSubscribe(Flow.Subscription subscription) {
            subscription.request(Long.MAX_VALUE);
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
        });
    return completion;
  }

  private static ReadOptions options() {
    return new ReadOptions(64, 1 << 20, ReadIsolation.COMMITTED, Duration.ofSeconds(10));
  }
}
