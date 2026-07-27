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

import com.nereusstream.api.Checksum;
import com.nereusstream.kafka.compaction.KafkaCompactionStreamingExecutor.StreamingResult;
import com.nereusstream.kafka.compaction.KafkaCompactionWriteRequestFactory.Input;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedObjectWriteRequest;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedObjectWriter;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectWriteResult;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/** Composes exact two-pass execution, checked row replay, and the streaming NTC2 Parquet writer. */
public final class KafkaCompactionParquetPublisher {
  private final KafkaCompactionStreamingExecutor executor;
  private final KafkaCompactionWriteRequestFactory requests;
  private final KafkaTopicCompactedObjectWriter writer;

  public KafkaCompactionParquetPublisher(
      KafkaCompactionStreamingExecutor executor,
      KafkaCompactionWriteRequestFactory requests,
      KafkaTopicCompactedObjectWriter writer) {
    this.executor = Objects.requireNonNull(executor, "executor");
    this.requests = Objects.requireNonNull(requests, "requests");
    this.writer = Objects.requireNonNull(writer, "writer");
  }

  public CompletableFuture<PreparedObject> prepare(
      KafkaCompactionPlan recoveredPlan,
      KafkaCompactionBatchSource.PassStreams streams,
      Input requestInput,
      boolean allowUncompressedFallback) {
    try {
      CompletableFuture<PreparedObject> result = new CompletableFuture<>();
      AtomicReference<CompletableFuture<?>> active = new AtomicReference<>();
      CompletableFuture<StreamingResult> execution =
          executor.execute(recoveredPlan, streams, allowUncompressedFallback);
      active.set(execution);
      result.whenComplete(
          (ignored, failure) -> {
            if (result.isCancelled()) {
              CompletableFuture<?> operation = active.get();
              if (operation != null) {
                operation.cancel(true);
              }
            }
          });
      execution.whenComplete(
          (streaming, executionFailure) -> {
            if (executionFailure != null) {
              result.completeExceptionally(executionFailure);
              return;
            }
            if (result.isDone()) {
              streaming.close();
              return;
            }
            KafkaTopicCompactedObjectWriteRequest request;
            try {
              request = requests.create(requestInput, streaming);
            } catch (Throwable failure) {
              streaming.close();
              result.completeExceptionally(failure);
              return;
            }
            CompletableFuture<RangedCompactedObjectWriteResult> writing;
            try {
              writing =
                  Objects.requireNonNull(
                      writer.write(request, streaming.rows()), "NTC2 writer future");
            } catch (Throwable failure) {
              streaming.close();
              result.completeExceptionally(failure);
              return;
            }
            active.set(writing);
            if (result.isDone()) {
              writing.cancel(true);
            }
            writing.whenComplete(
                (written, writeFailure) -> {
                  streaming.close();
                  if (writeFailure != null) {
                    result.completeExceptionally(writeFailure);
                    return;
                  }
                  PreparedObject prepared;
                  try {
                    prepared = new PreparedObject(request, written, Evidence.from(streaming));
                  } catch (Throwable failure) {
                    if (written != null) {
                      written.close();
                    }
                    result.completeExceptionally(failure);
                    return;
                  }
                  if (!result.complete(prepared)) {
                    prepared.close();
                  }
                });
          });
      return result;
    } catch (Throwable failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  public record PreparedObject(
      KafkaTopicCompactedObjectWriteRequest request,
      RangedCompactedObjectWriteResult written,
      Evidence evidence)
      implements AutoCloseable {
    public PreparedObject {
      Objects.requireNonNull(request, "request");
      Objects.requireNonNull(written, "written");
      Objects.requireNonNull(evidence, "evidence");
      if (written.outputEntryCount() != request.entryCount()
          || written.outputRecordCount() != request.outputRecordCount()
          || !written.contentSha256().equals(written.stagingFile().contentSha256())) {
        throw new IllegalArgumentException("prepared NTC2 object differs from verified accounting");
      }
    }

    @Override
    public void close() {
      written.close();
    }
  }

  public record Evidence(
      long decisionSourceBatchCount,
      long outputSourceBatchCount,
      Checksum decisionSourceSetSha256,
      Checksum outputSourceSetSha256,
      Checksum fullFactSha256,
      Checksum outputFactSha256,
      long spillRunCount,
      long peakInMemoryKeyBytes) {
    public Evidence {
      Objects.requireNonNull(decisionSourceSetSha256, "decisionSourceSetSha256");
      Objects.requireNonNull(outputSourceSetSha256, "outputSourceSetSha256");
      Objects.requireNonNull(fullFactSha256, "fullFactSha256");
      Objects.requireNonNull(outputFactSha256, "outputFactSha256");
      if (decisionSourceBatchCount <= 0
          || outputSourceBatchCount <= 0
          || spillRunCount < 0
          || peakInMemoryKeyBytes < 0) {
        throw new IllegalArgumentException("invalid Kafka compaction publication evidence");
      }
    }

    private static Evidence from(StreamingResult result) {
      return new Evidence(
          result.decisionSourceBatchCount(),
          result.outputSourceBatchCount(),
          result.decisionSourceSetSha256(),
          result.outputSourceSetSha256(),
          result.fullFactSha256(),
          result.outputFactSha256(),
          result.spillRunCount(),
          result.peakInMemoryKeyBytes());
    }
  }
}
