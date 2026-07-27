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

import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.materialization.ExactSourceRangeReader;
import com.nereusstream.materialization.ExactSourceSetBatchPublisher;
import java.util.Objects;
import java.util.concurrent.Executor;

/** Opens the two cold, exact source streams frozen by one recovered KCP1 plan. */
public final class KafkaCompactionBatchSource {
  private final ReaderFactory readers;
  private final ReadOptions options;
  private final Executor callbackExecutor;

  public KafkaCompactionBatchSource(
      ExactSourceRangeReader reader, ReadOptions options, Executor callbackExecutor) {
    this(ignored -> Objects.requireNonNull(reader, "reader"), options, callbackExecutor);
  }

  public KafkaCompactionBatchSource(
      ReaderFactory readers, ReadOptions options, Executor callbackExecutor) {
    this.readers = Objects.requireNonNull(readers, "readers");
    this.options = Objects.requireNonNull(options, "options");
    this.callbackExecutor = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
  }

  public PassStreams open(KafkaCompactionPlan recoveredPlan) {
    KafkaCompactionPlan plan = Objects.requireNonNull(recoveredPlan, "recoveredPlan");
    ExactSourceRangeReader reader =
        Objects.requireNonNull(
            readers.create(plan.streamId()), "Kafka exact-source reader factory returned null");
    return new PassStreams(
        new ExactSourceSetBatchPublisher(
            plan.decisionSources(), reader, options, callbackExecutor, true),
        new ExactSourceSetBatchPublisher(
            plan.outputSources(), reader, options, callbackExecutor, true));
  }

  @FunctionalInterface
  public interface ReaderFactory {
    ExactSourceRangeReader create(StreamId streamId);
  }

  public record PassStreams(
      ExactSourceSetBatchPublisher decisionHorizon, ExactSourceSetBatchPublisher outputCoverage)
      implements AutoCloseable {
    public PassStreams {
      Objects.requireNonNull(decisionHorizon, "decisionHorizon");
      Objects.requireNonNull(outputCoverage, "outputCoverage");
      if (decisionHorizon == outputCoverage) {
        throw new IllegalArgumentException(
            "Kafka compaction passes require independent cold streams");
      }
    }

    @Override
    public void close() {
      decisionHorizon.close();
      outputCoverage.close();
    }
  }
}
