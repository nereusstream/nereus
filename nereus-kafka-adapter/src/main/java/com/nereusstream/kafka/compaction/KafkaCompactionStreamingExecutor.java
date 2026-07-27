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
import com.nereusstream.api.ReadBatch;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.Facts;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.PassTwoVerifier;
import com.nereusstream.kafka.compaction.KafkaCompactionTwoPassExecutor.Limits;
import com.nereusstream.materialization.CompactionRewriteContext;
import com.nereusstream.materialization.ExactSourceSet;
import com.nereusstream.materialization.ExactSourceSetVerifier;
import com.nereusstream.materialization.RewrittenCompactionRecord;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedObjectRow;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.record.RecordBatch;

/**
 * Production two-source-pass executor with bounded winner spill and a demand-driven row spool.
 *
 * <p>Pass one consumes the full KCP1 decision stream. Pass two consumes the exact output prefix,
 * verifies it against pass-one facts, and writes retained rewrites to a checksummed row spool. The
 * returned publisher is then consumed once by the NTC2 Parquet writer.
 */
public final class KafkaCompactionStreamingExecutor {
  private final KafkaTopicCompactionCodecV1 codec;
  private final KafkaCompactionStrategyV1 strategy;
  private final KafkaCompactionRowMapper rowMapper;
  private final Limits limits;
  private final StagingFileManager stagingFiles;
  private final Executor callbackExecutor;

  public KafkaCompactionStreamingExecutor(
      KafkaTopicCompactionCodecV1 codec,
      KafkaCompactionStrategyV1 strategy,
      KafkaCompactionRowMapper rowMapper,
      Limits limits,
      StagingFileManager stagingFiles,
      Executor callbackExecutor) {
    this.codec = Objects.requireNonNull(codec, "codec");
    this.strategy = Objects.requireNonNull(strategy, "strategy");
    this.rowMapper = Objects.requireNonNull(rowMapper, "rowMapper");
    this.limits = Objects.requireNonNull(limits, "limits");
    this.stagingFiles = Objects.requireNonNull(stagingFiles, "stagingFiles");
    this.callbackExecutor = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
  }

  public CompletableFuture<StreamingResult> execute(
      KafkaCompactionPlan recoveredPlan,
      KafkaCompactionBatchSource.PassStreams streams,
      boolean allowUncompressedFallback) {
    try {
      KafkaCompactionPlan plan = Objects.requireNonNull(recoveredPlan, "recoveredPlan");
      KafkaCompactionBatchSource.PassStreams exactStreams =
          Objects.requireNonNull(streams, "streams");
      DecisionStage decision =
          new DecisionStage(
              plan.decisionSources(), plan.passOneSnapshot(), exactStreams.decisionHorizon());
      CompletableFuture<StreamingResult> result = new CompletableFuture<>();
      AtomicReference<CompletableFuture<?>> active = new AtomicReference<>();
      result.whenComplete(
          (ignored, failure) -> {
            if (result.isCancelled()) {
              CompletableFuture<?> operation = active.get();
              if (operation != null) {
                operation.cancel(true);
              }
            }
            exactStreams.close();
          });
      CompletableFuture<PreparedFacts> deciding = decision.start();
      active.set(deciding);
      deciding.whenComplete(
          (prepared, decisionFailure) -> {
            if (decisionFailure != null) {
              result.completeExceptionally(decisionFailure);
              return;
            }
            if (result.isDone()) {
              return;
            }
            OutputStage output;
            try {
              output =
                  new OutputStage(
                      plan.outputSources(),
                      prepared.facts(),
                      exactStreams.outputCoverage(),
                      allowUncompressedFallback);
            } catch (Throwable failure) {
              result.completeExceptionally(failure);
              return;
            }
            CompletableFuture<SpooledOutput> producing = output.start();
            active.set(producing);
            if (result.isDone()) {
              producing.cancel(true);
              return;
            }
            producing.whenComplete(
                (spooled, outputFailure) -> {
                  if (outputFailure != null) {
                    result.completeExceptionally(outputFailure);
                    return;
                  }
                  StreamingResult completed;
                  try {
                    completed = streamingResult(plan, prepared, spooled);
                  } catch (Throwable failure) {
                    spooled.close();
                    result.completeExceptionally(failure);
                    return;
                  }
                  if (!result.complete(completed)) {
                    completed.close();
                  }
                });
          });
      return result;
    } catch (Throwable failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private StreamingResult streamingResult(
      KafkaCompactionPlan plan, PreparedFacts prepared, SpooledOutput spooled) {
    KafkaCompactionRowSpool.Spool rows = spooled.rows();
    boolean success = false;
    try {
      StreamingResult result =
          new StreamingResult(
              rows,
              plan.passOneSnapshot().outputCoverage(),
              plan.passOneSnapshot().decisionHorizon(),
              prepared.sourceBatches(),
              spooled.sourceBatches(),
              rows.outputRecords(),
              rows.rows(),
              rows.logicalBytes(),
              plan.decisionSources().sourceSetSha256(),
              plan.outputSources().sourceSetSha256(),
              prepared.facts().fullFactSha256(),
              prepared.facts().outputFactSha256(),
              prepared.facts().spillRunCount(),
              prepared.facts().peakInMemoryKeyBytes());
      success = true;
      return result;
    } finally {
      if (!success) {
        rows.close();
      }
    }
  }

  private final class DecisionStage extends BatchStage<PreparedFacts> {
    private final ExactSourceSetVerifier sourceVerifier;
    private final KafkaCompactionPassOneCollector collector;

    private DecisionStage(
        ExactSourceSet sources,
        KafkaCompactionPassOneCollector.Snapshot snapshot,
        Flow.Publisher<ReadBatch> batches) {
      super(batches);
      ExactSourceSet exactSources = Objects.requireNonNull(sources, "decisionSources");
      if (!exactSources.coverage().equals(snapshot.decisionHorizon())) {
        throw new IllegalArgumentException(
            "Kafka compaction decision stream does not match KCP1 horizon");
      }
      sourceVerifier = new ExactSourceSetVerifier(exactSources);
      collector = new KafkaCompactionPassOneCollector(snapshot, stagingFiles);
    }

    @Override
    void process(ReadBatch batch) {
      sourceVerifier.accept(batch);
      codec.decode(batch, collector::accept);
    }

    @Override
    PreparedFacts finishStage() {
      sourceVerifier.finish();
      return new PreparedFacts(collector.finish(), sourceBatches());
    }

    @Override
    void closeStage() {
      collector.close();
    }
  }

  private final class OutputStage extends BatchStage<SpooledOutput> {
    private final ExactSourceSetVerifier sourceVerifier;
    private final Facts facts;
    private final PassTwoVerifier verifier;
    private final boolean allowUncompressedFallback;
    private final KafkaCompactionRowSpool.Writer spool;

    private OutputStage(
        ExactSourceSet outputSources,
        Facts facts,
        Flow.Publisher<ReadBatch> batches,
        boolean allowUncompressedFallback) {
      super(batches);
      ExactSourceSet exactSources = Objects.requireNonNull(outputSources, "outputSources");
      this.facts = Objects.requireNonNull(facts, "facts");
      if (!exactSources.coverage().equals(facts.outputCoverage())) {
        throw new IllegalArgumentException(
            "Kafka compaction output stream does not match pass-one coverage");
      }
      this.sourceVerifier = new ExactSourceSetVerifier(exactSources);
      this.verifier = facts.newPassTwoVerifier();
      this.allowUncompressedFallback = allowUncompressedFallback;
      this.spool =
          KafkaCompactionRowSpool.writer(
              stagingFiles,
              facts.outputCoverage(),
              limits.maxOutputBatches(),
              limits.maxOutputBytes());
    }

    @Override
    void process(ReadBatch batch) {
      sourceVerifier.accept(batch);
      codec.decode(
          batch,
          record -> {
            verifier.accept(record);
            KafkaCompactionStrategyV1.Decision decision =
                strategy.decide(record, facts.contextFor(record));
            if (decision.retained()) {
              RewrittenCompactionRecord rewritten =
                  codec.rewrite(
                      record,
                      new CompactionRewriteContext(
                          RecordBatch.MAGIC_VALUE_V2,
                          codec.messageFormatSha256(),
                          allowUncompressedFallback,
                          facts.rewriteDeleteHorizon(record)));
              KafkaTopicCompactedObjectRow row = rowMapper.toNtc2Row(rewritten);
              spool.append(row);
            }
          });
    }

    @Override
    SpooledOutput finishStage() {
      sourceVerifier.finish();
      verifier.finish();
      return new SpooledOutput(spool.finish(callbackExecutor), sourceBatches());
    }

    @Override
    void closeStage() {
      spool.close();
    }
  }

  private abstract class BatchStage<T> implements Flow.Subscriber<ReadBatch> {
    private final Flow.Publisher<ReadBatch> batches;
    private final SerialExecutor serial = new SerialExecutor(callbackExecutor);
    private final CompletableFuture<T> result = new CompletableFuture<>();
    private final AtomicBoolean subscribed = new AtomicBoolean();
    private Flow.Subscription subscription;
    private long sourceBatches;
    private boolean awaiting;
    private boolean terminal;

    private BatchStage(Flow.Publisher<ReadBatch> batches) {
      this.batches = Objects.requireNonNull(batches, "batches");
      result.whenComplete(
          (ignored, failure) -> {
            if (result.isCancelled()) {
              submit(this::cancelOnSerial);
            }
          });
    }

    final CompletableFuture<T> start() {
      try {
        batches.subscribe(this);
      } catch (Throwable failure) {
        fail(failure);
      }
      return result;
    }

    final long sourceBatches() {
      return sourceBatches;
    }

    abstract void process(ReadBatch batch);

    abstract T finishStage();

    abstract void closeStage();

    @Override
    public final void onSubscribe(Flow.Subscription value) {
      Objects.requireNonNull(value, "subscription");
      if (!subscribed.compareAndSet(false, true)) {
        value.cancel();
        fail(new IllegalStateException("Kafka compaction batch stage subscribed more than once"));
        return;
      }
      submit(
          () -> {
            if (terminal) {
              value.cancel();
              return;
            }
            subscription = value;
            requestOne();
          });
    }

    @Override
    public final void onNext(ReadBatch batch) {
      submit(
          () -> {
            if (terminal) {
              return;
            }
            if (!awaiting) {
              failOnSerial(
                  new IllegalStateException(
                      "Kafka compaction source emitted a batch without demand"));
              return;
            }
            awaiting = false;
            try {
              sourceBatches = Math.addExact(sourceBatches, 1);
              if (sourceBatches > limits.maxSourceBatches()) {
                throw new IllegalArgumentException(
                    "Kafka compaction source exceeded its batch limit");
              }
              process(Objects.requireNonNull(batch, "batch"));
              requestOne();
            } catch (Throwable failure) {
              failOnSerial(failure);
            }
          });
    }

    @Override
    public final void onError(Throwable failure) {
      submit(() -> failOnSerial(Objects.requireNonNull(failure, "failure")));
    }

    @Override
    public final void onComplete() {
      submit(
          () -> {
            if (terminal) {
              return;
            }
            terminal = true;
            awaiting = false;
            try {
              T completed = Objects.requireNonNull(finishStage(), "stage result");
              if (!result.complete(completed) && completed instanceof AutoCloseable closeable) {
                closeQuietly(closeable);
              }
            } catch (Throwable failure) {
              closeStageAfterFailure(failure);
              result.completeExceptionally(failure);
            }
          });
    }

    private void requestOne() {
      if (terminal) {
        return;
      }
      if (subscription == null || awaiting) {
        failOnSerial(
            new IllegalStateException("Kafka compaction source subscription state is invalid"));
        return;
      }
      awaiting = true;
      try {
        subscription.request(1);
      } catch (Throwable failure) {
        awaiting = false;
        failOnSerial(failure);
      }
    }

    private void fail(Throwable failure) {
      submit(() -> failOnSerial(failure));
    }

    private void failOnSerial(Throwable failure) {
      if (terminal) {
        return;
      }
      terminal = true;
      awaiting = false;
      if (subscription != null) {
        subscription.cancel();
      }
      closeStageAfterFailure(failure);
      result.completeExceptionally(failure);
    }

    private void cancelOnSerial() {
      if (terminal) {
        return;
      }
      terminal = true;
      awaiting = false;
      if (subscription != null) {
        subscription.cancel();
      }
      closeStageAfterFailure(null);
    }

    private void submit(Runnable action) {
      try {
        serial.execute(action);
      } catch (RejectedExecutionException failure) {
        if (!terminal) {
          terminal = true;
          closeStageAfterFailure(failure);
          result.completeExceptionally(failure);
        }
      }
    }

    private void closeStageAfterFailure(Throwable original) {
      try {
        closeStage();
      } catch (Throwable closeFailure) {
        if (original != null) {
          original.addSuppressed(closeFailure);
        }
      }
    }
  }

  public static final class StreamingResult implements AutoCloseable {
    private final KafkaCompactionRowSpool.Spool rows;
    private final com.nereusstream.api.OffsetRange outputCoverage;
    private final com.nereusstream.api.OffsetRange decisionHorizon;
    private final long decisionSourceBatchCount;
    private final long outputSourceBatchCount;
    private final long outputRecordCount;
    private final int outputBatchCount;
    private final long logicalBytes;
    private final Checksum decisionSourceSetSha256;
    private final Checksum outputSourceSetSha256;
    private final Checksum fullFactSha256;
    private final Checksum outputFactSha256;
    private final long spillRunCount;
    private final long peakInMemoryKeyBytes;

    private StreamingResult(
        KafkaCompactionRowSpool.Spool rows,
        com.nereusstream.api.OffsetRange outputCoverage,
        com.nereusstream.api.OffsetRange decisionHorizon,
        long decisionSourceBatchCount,
        long outputSourceBatchCount,
        long outputRecordCount,
        int outputBatchCount,
        long logicalBytes,
        Checksum decisionSourceSetSha256,
        Checksum outputSourceSetSha256,
        Checksum fullFactSha256,
        Checksum outputFactSha256,
        long spillRunCount,
        long peakInMemoryKeyBytes) {
      this.rows = Objects.requireNonNull(rows, "rows");
      this.outputCoverage = Objects.requireNonNull(outputCoverage, "outputCoverage");
      this.decisionHorizon = Objects.requireNonNull(decisionHorizon, "decisionHorizon");
      this.decisionSourceSetSha256 =
          Objects.requireNonNull(decisionSourceSetSha256, "decisionSourceSetSha256");
      this.outputSourceSetSha256 =
          Objects.requireNonNull(outputSourceSetSha256, "outputSourceSetSha256");
      this.fullFactSha256 = Objects.requireNonNull(fullFactSha256, "fullFactSha256");
      this.outputFactSha256 = Objects.requireNonNull(outputFactSha256, "outputFactSha256");
      if (outputCoverage.isEmpty()
          || decisionHorizon.isEmpty()
          || outputCoverage.startOffset() != decisionHorizon.startOffset()
          || outputCoverage.endOffset() > decisionHorizon.endOffset()
          || decisionSourceBatchCount <= 0
          || outputSourceBatchCount <= 0
          || outputRecordCount < 0
          || outputRecordCount > outputCoverage.recordCount()
          || outputBatchCount < 0
          || outputRecordCount != outputBatchCount
          || outputBatchCount != rows.rows()
          || logicalBytes != rows.logicalBytes()
          || outputRecordCount != rows.outputRecords()
          || spillRunCount < 0
          || peakInMemoryKeyBytes < 0) {
        throw new IllegalArgumentException("invalid Kafka streaming compaction result");
      }
      this.decisionSourceBatchCount = decisionSourceBatchCount;
      this.outputSourceBatchCount = outputSourceBatchCount;
      this.outputRecordCount = outputRecordCount;
      this.outputBatchCount = outputBatchCount;
      this.logicalBytes = logicalBytes;
      this.spillRunCount = spillRunCount;
      this.peakInMemoryKeyBytes = peakInMemoryKeyBytes;
    }

    public Flow.Publisher<KafkaTopicCompactedObjectRow> rows() {
      return rows;
    }

    public com.nereusstream.api.OffsetRange outputCoverage() {
      return outputCoverage;
    }

    public com.nereusstream.api.OffsetRange decisionHorizon() {
      return decisionHorizon;
    }

    public long decisionSourceBatchCount() {
      return decisionSourceBatchCount;
    }

    public long outputSourceBatchCount() {
      return outputSourceBatchCount;
    }

    public long outputRecordCount() {
      return outputRecordCount;
    }

    public int outputBatchCount() {
      return outputBatchCount;
    }

    public long logicalBytes() {
      return logicalBytes;
    }

    public Checksum decisionSourceSetSha256() {
      return decisionSourceSetSha256;
    }

    public Checksum outputSourceSetSha256() {
      return outputSourceSetSha256;
    }

    public Checksum fullFactSha256() {
      return fullFactSha256;
    }

    public Checksum outputFactSha256() {
      return outputFactSha256;
    }

    public long spillRunCount() {
      return spillRunCount;
    }

    public long peakInMemoryKeyBytes() {
      return peakInMemoryKeyBytes;
    }

    @Override
    public void close() {
      rows.close();
    }
  }

  private record PreparedFacts(Facts facts, long sourceBatches) {
    private PreparedFacts {
      Objects.requireNonNull(facts, "facts");
      if (sourceBatches <= 0) {
        throw new IllegalArgumentException("Kafka decision pass must consume source batches");
      }
    }
  }

  private record SpooledOutput(KafkaCompactionRowSpool.Spool rows, long sourceBatches)
      implements AutoCloseable {
    private SpooledOutput {
      Objects.requireNonNull(rows, "rows");
      if (sourceBatches <= 0) {
        throw new IllegalArgumentException("Kafka output pass must consume source batches");
      }
    }

    @Override
    public void close() {
      rows.close();
    }
  }

  private static void closeQuietly(AutoCloseable closeable) {
    try {
      closeable.close();
    } catch (Exception ignored) {
    }
  }

  private static final class SerialExecutor implements Executor {
    private final Executor delegate;
    private final ArrayDeque<Runnable> queue = new ArrayDeque<>();
    private boolean running;

    private SerialExecutor(Executor delegate) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public synchronized void execute(Runnable command) {
      queue.add(Objects.requireNonNull(command, "command"));
      if (running) {
        return;
      }
      running = true;
      try {
        delegate.execute(this::drain);
      } catch (RuntimeException failure) {
        running = false;
        queue.clear();
        throw failure;
      }
    }

    private void drain() {
      while (true) {
        Runnable next;
        synchronized (this) {
          next = queue.poll();
          if (next == null) {
            running = false;
            return;
          }
        }
        try {
          next.run();
        } catch (Throwable ignored) {
          // Stage actions capture their own failures.
        }
      }
    }
  }
}
