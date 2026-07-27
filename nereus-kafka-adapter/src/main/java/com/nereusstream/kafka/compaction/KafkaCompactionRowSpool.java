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
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV2;
import com.nereusstream.objectstore.compacted.KafkaCompactionDispositionV2;
import com.nereusstream.objectstore.compacted.KafkaCompactionKeyEncodingV2;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedObjectRow;
import com.nereusstream.objectstore.staging.PrivateStagingSpillFile;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HexFormat;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Checksum-verified execution-local spool between Kafka pass two and the streaming NTC2 writer.
 *
 * <p>The pass-two source is consumed once. Exact rewritten rows are then replayed once under
 * downstream demand, allowing the Parquet request to use terminally verified row and byte
 * accounting without retaining payloads in heap.
 */
final class KafkaCompactionRowSpool {
  private static final int MAGIC = 0x4b435253; // KCRS
  private static final int VERSION = 1;
  private static final long END = -1;
  private static final int BUFFER_BYTES = 64 << 10;
  private static final int SHA256_BYTES = 32;

  private KafkaCompactionRowSpool() {}

  static Writer writer(
      StagingFileManager stagingFiles,
      OffsetRange outputCoverage,
      int maxRows,
      long maxLogicalBytes) {
    return new Writer(
        Objects.requireNonNull(stagingFiles, "stagingFiles"),
        Objects.requireNonNull(outputCoverage, "outputCoverage"),
        maxRows,
        maxLogicalBytes);
  }

  static final class Writer implements AutoCloseable {
    private final OffsetRange outputCoverage;
    private final int maxRows;
    private final long maxLogicalBytes;
    private final PrivateStagingSpillFile file;
    private final DataOutputStream output;
    private long previousOffset = -1;
    private long logicalBytes;
    private long outputRecords;
    private int rows;
    private boolean finished;

    private Writer(
        StagingFileManager stagingFiles,
        OffsetRange outputCoverage,
        int maxRows,
        long maxLogicalBytes) {
      if (outputCoverage.isEmpty() || maxRows <= 0 || maxLogicalBytes <= 0) {
        throw new IllegalArgumentException("invalid Kafka compaction row-spool limits");
      }
      this.outputCoverage = outputCoverage;
      this.maxRows = maxRows;
      this.maxLogicalBytes = maxLogicalBytes;
      this.file = stagingFiles.createSpill("kafka-rows");
      DataOutputStream opened = null;
      try {
        opened = new DataOutputStream(new BufferedOutputStream(file.outputStream(), BUFFER_BYTES));
        opened.writeInt(MAGIC);
        opened.writeInt(VERSION);
      } catch (Throwable failure) {
        closeQuietly(opened);
        file.close();
        throw storageFailure("initialize Kafka compaction row spool", failure);
      }
      output = opened;
    }

    void append(KafkaTopicCompactedObjectRow row) {
      if (finished) {
        throw new IllegalStateException("Kafka compaction row spool is already finished");
      }
      KafkaTopicCompactedObjectRow exact = Objects.requireNonNull(row, "row");
      byte[] key = bytes(exact.compactionKey());
      byte[] payload = bytes(exact.exactPayload());
      byte[] sourceSha = HexFormat.of().parseHex(exact.sourceBatchSha256().value());
      if (!outputCoverage.contains(exact.streamOffsetStart())
          || exact.endOffset() > outputCoverage.endOffset()
          || exact.streamOffsetStart() <= previousOffset
          || key.length <= 0
          || key.length > KafkaCompactionKeyEncodingV2.MAX_ENCODED_KEY_BYTES
          || payload.length <= 0
          || payload.length > CompactedObjectFormatV2.MAX_PAYLOAD_BYTES
          || sourceSha.length != SHA256_BYTES) {
        throw new IllegalArgumentException("invalid Kafka compaction row-spool input");
      }
      int nextRows = Math.addExact(rows, 1);
      long nextRecords = Math.addExact(outputRecords, exact.recordCount());
      long nextLogicalBytes = Math.addExact(logicalBytes, payload.length);
      if (nextRows > maxRows || nextLogicalBytes > maxLogicalBytes) {
        throw new IllegalArgumentException("Kafka compaction row spool exceeded frozen limits");
      }
      try {
        output.writeLong(exact.streamOffsetStart());
        output.writeInt(exact.recordCount());
        output.writeInt(exact.disposition().wireId());
        output.writeInt(key.length);
        output.write(key);
        output.writeInt(payload.length);
        output.write(payload);
        output.writeInt(exact.payloadCrc32c());
        output.writeLong(exact.sourceBatchBaseOffset());
        output.writeInt(exact.sourceRecordIndex());
        output.write(sourceSha);
        output.writeBoolean(exact.eventTimeMillis().isPresent());
        if (exact.eventTimeMillis().isPresent()) {
          output.writeLong(exact.eventTimeMillis().getAsLong());
        }
      } catch (IOException failure) {
        throw storageFailure("write Kafka compaction row spool", failure);
      }
      previousOffset = exact.streamOffsetStart();
      rows = nextRows;
      outputRecords = nextRecords;
      logicalBytes = nextLogicalBytes;
    }

    Spool finish(Executor readerExecutor) {
      if (finished) {
        throw new IllegalStateException("Kafka compaction row spool is already finished");
      }
      Executor exactExecutor = Objects.requireNonNull(readerExecutor, "readerExecutor");
      try {
        output.writeLong(END);
        output.writeInt(rows);
        output.writeLong(outputRecords);
        output.writeLong(logicalBytes);
        output.close();
        file.seal();
        finished = true;
        return new Spool(file, outputCoverage, rows, outputRecords, logicalBytes, exactExecutor);
      } catch (IOException failure) {
        throw storageFailure("finish Kafka compaction row spool", failure);
      }
    }

    @Override
    public void close() {
      if (finished) {
        return;
      }
      finished = true;
      try {
        output.close();
      } catch (IOException ignored) {
      }
      file.close();
    }
  }

  static final class Spool implements Flow.Publisher<KafkaTopicCompactedObjectRow>, AutoCloseable {
    private final PrivateStagingSpillFile file;
    private final OffsetRange outputCoverage;
    private final int rows;
    private final long outputRecords;
    private final long logicalBytes;
    private final Executor executor;
    private final AtomicBoolean subscribed = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile RowSubscription active;

    private Spool(
        PrivateStagingSpillFile file,
        OffsetRange outputCoverage,
        int rows,
        long outputRecords,
        long logicalBytes,
        Executor executor) {
      this.file = Objects.requireNonNull(file, "file");
      this.outputCoverage = Objects.requireNonNull(outputCoverage, "outputCoverage");
      this.rows = rows;
      this.outputRecords = outputRecords;
      this.logicalBytes = logicalBytes;
      this.executor = Objects.requireNonNull(executor, "executor");
    }

    int rows() {
      return rows;
    }

    long outputRecords() {
      return outputRecords;
    }

    long logicalBytes() {
      return logicalBytes;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super KafkaTopicCompactedObjectRow> subscriber) {
      Objects.requireNonNull(subscriber, "subscriber");
      if (!subscribed.compareAndSet(false, true) || closed.get()) {
        reject(subscriber, "Kafka compaction row spool permits one live subscriber");
        return;
      }
      try {
        RowSubscription subscription =
            new RowSubscription(
                subscriber,
                new DataInputStream(
                    new BufferedInputStream(file.openVerifiedInputStream(), BUFFER_BYTES)));
        active = subscription;
        subscriber.onSubscribe(subscription);
      } catch (Throwable failure) {
        close();
        reject(subscriber, "cannot open Kafka compaction row spool", failure);
      }
    }

    @Override
    public void close() {
      if (!closed.compareAndSet(false, true)) {
        return;
      }
      RowSubscription subscription = active;
      if (subscription != null) {
        subscription.cancel();
      }
      file.close();
    }

    private final class RowSubscription implements Flow.Subscription {
      private final Flow.Subscriber<? super KafkaTopicCompactedObjectRow> downstream;
      private final DataInputStream input;
      private long demand;
      private boolean draining;
      private boolean terminal;
      private int emittedRows;
      private long emittedRecords;
      private long emittedLogicalBytes;
      private long previousOffset = -1;

      private RowSubscription(
          Flow.Subscriber<? super KafkaTopicCompactedObjectRow> downstream, DataInputStream input)
          throws IOException {
        this.downstream = downstream;
        this.input = input;
        if (input.readInt() != MAGIC || input.readInt() != VERSION) {
          throw new IOException("Kafka compaction row-spool header is invalid");
        }
      }

      @Override
      public void request(long count) {
        boolean invalid = false;
        synchronized (this) {
          if (terminal) {
            return;
          }
          if (count <= 0) {
            invalid = true;
          } else {
            demand = addDemand(demand, count);
            if (draining) {
              return;
            }
            draining = true;
          }
        }
        if (invalid) {
          fail(new IllegalArgumentException("Flow request count must be positive"));
          return;
        }
        submit(this::drain);
      }

      @Override
      public void cancel() {
        synchronized (this) {
          if (terminal) {
            return;
          }
          terminal = true;
        }
        closeInput();
        Spool.this.close();
      }

      private void drain() {
        while (true) {
          synchronized (this) {
            if (terminal) {
              draining = false;
              return;
            }
            if (demand == 0) {
              draining = false;
              return;
            }
            demand--;
          }
          KafkaTopicCompactedObjectRow next;
          try {
            next = readNext();
          } catch (Throwable failure) {
            fail(failure);
            return;
          }
          if (next == null) {
            complete();
            return;
          }
          try {
            downstream.onNext(next);
          } catch (Throwable failure) {
            fail(failure);
            return;
          }
        }
      }

      private KafkaTopicCompactedObjectRow readNext() throws IOException {
        long offset = input.readLong();
        if (offset == END) {
          int declaredRows = input.readInt();
          long declaredRecords = input.readLong();
          long declaredBytes = input.readLong();
          if (declaredRows != rows
              || declaredRecords != outputRecords
              || declaredBytes != logicalBytes
              || emittedRows != rows
              || emittedRecords != outputRecords
              || emittedLogicalBytes != logicalBytes
              || input.read() != -1) {
            throw new IOException("Kafka compaction row-spool terminal accounting differs");
          }
          return null;
        }
        if (!outputCoverage.contains(offset) || offset <= previousOffset) {
          throw new IOException("Kafka compaction row-spool offsets are invalid");
        }
        int recordCount = input.readInt();
        KafkaCompactionDispositionV2 disposition =
            KafkaCompactionDispositionV2.fromWireId(input.readInt());
        int keyLength = input.readInt();
        byte[] key =
            readBytes(keyLength, KafkaCompactionKeyEncodingV2.MAX_ENCODED_KEY_BYTES, "key");
        int payloadLength = input.readInt();
        byte[] payload =
            readBytes(payloadLength, CompactedObjectFormatV2.MAX_PAYLOAD_BYTES, "payload");
        int payloadCrc32c = input.readInt();
        long sourceBatchBaseOffset = input.readLong();
        int sourceRecordIndex = input.readInt();
        byte[] sourceSha = input.readNBytes(SHA256_BYTES);
        if (sourceSha.length != SHA256_BYTES) {
          throw new EOFException("Kafka compaction row-spool source digest is truncated");
        }
        OptionalLong eventTime =
            input.readBoolean() ? OptionalLong.of(input.readLong()) : OptionalLong.empty();
        KafkaTopicCompactedObjectRow row =
            new KafkaTopicCompactedObjectRow(
                offset,
                recordCount,
                disposition,
                ByteBuffer.wrap(key),
                ByteBuffer.wrap(payload),
                payloadCrc32c,
                sourceBatchBaseOffset,
                sourceRecordIndex,
                new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(sourceSha)),
                eventTime);
        if (row.endOffset() > outputCoverage.endOffset()) {
          throw new IOException("Kafka compaction row-spool row exceeds output coverage");
        }
        emittedRows = Math.addExact(emittedRows, 1);
        emittedRecords = Math.addExact(emittedRecords, recordCount);
        emittedLogicalBytes = Math.addExact(emittedLogicalBytes, payloadLength);
        if (emittedRows > rows
            || emittedRecords > outputRecords
            || emittedLogicalBytes > logicalBytes) {
          throw new IOException("Kafka compaction row-spool exceeded terminal accounting");
        }
        previousOffset = offset;
        return row;
      }

      private byte[] readBytes(int length, int maxLength, String field) throws IOException {
        if (length <= 0 || length > maxLength) {
          throw new IOException("Kafka compaction row-spool " + field + " length is invalid");
        }
        byte[] value = input.readNBytes(length);
        if (value.length != length) {
          throw new EOFException("Kafka compaction row-spool " + field + " is truncated");
        }
        return value;
      }

      private void complete() {
        synchronized (this) {
          if (terminal) {
            return;
          }
          terminal = true;
          draining = false;
        }
        closeInput();
        file.close();
        closed.set(true);
        downstream.onComplete();
      }

      private void fail(Throwable failure) {
        synchronized (this) {
          if (terminal) {
            return;
          }
          terminal = true;
          draining = false;
        }
        closeInput();
        file.close();
        closed.set(true);
        downstream.onError(storageFailure("read Kafka compaction row spool", failure));
      }

      private void closeInput() {
        try {
          input.close();
        } catch (IOException ignored) {
        }
      }

      private void submit(Runnable action) {
        try {
          executor.execute(action);
        } catch (RejectedExecutionException failure) {
          fail(
              new NereusException(
                  ErrorCode.STORAGE_CLOSED,
                  false,
                  "Kafka compaction row-spool executor rejected demand",
                  failure));
        }
      }
    }
  }

  private static void reject(Flow.Subscriber<?> subscriber, String message) {
    reject(subscriber, message, new IllegalStateException(message));
  }

  private static void reject(Flow.Subscriber<?> subscriber, String message, Throwable failure) {
    try {
      subscriber.onSubscribe(
          new Flow.Subscription() {
            @Override
            public void request(long count) {}

            @Override
            public void cancel() {}
          });
      subscriber.onError(storageFailure(message, failure));
    } catch (Throwable ignored) {
    }
  }

  private static long addDemand(long current, long added) {
    long result = current + added;
    return result < 0 ? Long.MAX_VALUE : result;
  }

  private static byte[] bytes(ByteBuffer value) {
    ByteBuffer exact = Objects.requireNonNull(value, "value").asReadOnlyBuffer();
    byte[] bytes = new byte[exact.remaining()];
    exact.get(bytes);
    return bytes;
  }

  private static void closeQuietly(DataOutputStream output) {
    if (output == null) {
      return;
    }
    try {
      output.close();
    } catch (IOException ignored) {
    }
  }

  private static NereusException storageFailure(String message, Throwable cause) {
    if (cause instanceof NereusException nereus) {
      return nereus;
    }
    return new NereusException(ErrorCode.OBJECT_READ_FAILED, true, message, cause);
  }
}
