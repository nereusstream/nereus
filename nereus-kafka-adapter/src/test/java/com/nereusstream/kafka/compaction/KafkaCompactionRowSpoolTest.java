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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.compacted.KafkaCompactionDispositionV2;
import com.nereusstream.objectstore.compacted.KafkaCompactionKeyEncodingV2;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedObjectRow;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaCompactionRowSpoolTest {
  @TempDir Path temporaryDirectory;

  @Test
  void replaysRowsOneDemandAtATimeAndReleasesTheSpoolAtVerifiedEof() throws Exception {
    Path stagingPath = stagingDirectory("replay");
    try (StagingFileManager staging = staging(stagingPath)) {
      KafkaCompactionRowSpool.Writer writer =
          KafkaCompactionRowSpool.writer(staging, new OffsetRange(10, 20), 10, 1 << 20);
      writer.append(row(10, "a"));
      writer.append(row(15, "b"));
      KafkaCompactionRowSpool.Spool spool = writer.finish(Runnable::run);
      assertThat(staging.reservedBytes()).isPositive();

      ArrayList<Long> offsets = new ArrayList<>();
      CompletableFuture<Void> completion = new CompletableFuture<>();
      spool.subscribe(
          new Flow.Subscriber<>() {
            private Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription value) {
              subscription = value;
              value.request(1);
            }

            @Override
            public void onNext(KafkaTopicCompactedObjectRow item) {
              offsets.add(item.streamOffsetStart());
              subscription.request(1);
            }

            @Override
            public void onError(Throwable failure) {
              completion.completeExceptionally(failure);
            }

            @Override
            public void onComplete() {
              completion.complete(null);
            }
          });

      completion.join();
      assertThat(offsets).containsExactly(10L, 15L);
      assertThat(staging.reservedBytes()).isZero();
      try (var files = Files.list(stagingPath)) {
        assertThat(files.toList()).isEmpty();
      }
    }
  }

  @Test
  void sameLengthMutationFailsWholeFileVerificationAndCleansTheSpool() throws Exception {
    Path stagingPath = stagingDirectory("corrupt");
    try (StagingFileManager staging = staging(stagingPath)) {
      KafkaCompactionRowSpool.Writer writer =
          KafkaCompactionRowSpool.writer(staging, new OffsetRange(10, 20), 10, 1 << 20);
      writer.append(row(10, "x"));
      KafkaCompactionRowSpool.Spool spool = writer.finish(Runnable::run);
      Path file;
      try (var files = Files.list(stagingPath)) {
        file = files.findFirst().orElseThrow();
      }
      try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
        channel.write(ByteBuffer.wrap(new byte[] {(byte) 0xbb}), 50);
        channel.force(true);
      }

      CompletableFuture<Void> completion = consume(spool);

      assertThat(completion).isCompletedExceptionally();
      assertThat(staging.reservedBytes()).isZero();
      assertThat(Files.exists(file)).isFalse();
    }
  }

  @Test
  void closeBeforeSubscriptionDropsEveryReservedByte() throws Exception {
    Path stagingPath = stagingDirectory("close");
    try (StagingFileManager staging = staging(stagingPath)) {
      KafkaCompactionRowSpool.Writer writer =
          KafkaCompactionRowSpool.writer(staging, new OffsetRange(10, 20), 10, 1 << 20);
      writer.append(row(10, "x"));
      KafkaCompactionRowSpool.Spool spool = writer.finish(Runnable::run);
      assertThat(staging.reservedBytes()).isPositive();

      spool.close();

      assertThat(staging.reservedBytes()).isZero();
      try (var files = Files.list(stagingPath)) {
        assertThat(files.toList()).isEmpty();
      }
    }
  }

  @Test
  void nullReplayExecutorIsRejectedBeforeSealAndExplicitCloseReleasesTheSpool() throws Exception {
    Path stagingPath = stagingDirectory("null-executor");
    try (StagingFileManager staging = staging(stagingPath)) {
      KafkaCompactionRowSpool.Writer writer =
          KafkaCompactionRowSpool.writer(staging, new OffsetRange(10, 20), 10, 1 << 20);
      writer.append(row(10, "x"));

      assertThatThrownBy(() -> writer.finish(null))
          .isInstanceOf(NullPointerException.class)
          .hasMessage("readerExecutor");
      writer.close();

      assertThat(staging.reservedBytes()).isZero();
      try (var files = Files.list(stagingPath)) {
        assertThat(files.toList()).isEmpty();
      }
    }
  }

  @Test
  void emptySpoolStillVerifiesItsTerminalTupleAndReleasesTheFile() throws Exception {
    Path stagingPath = stagingDirectory("empty");
    try (StagingFileManager staging = staging(stagingPath)) {
      KafkaCompactionRowSpool.Spool spool =
          KafkaCompactionRowSpool.writer(staging, new OffsetRange(10, 20), 10, 1 << 20)
              .finish(Runnable::run);

      consume(spool).join();

      assertThat(staging.reservedBytes()).isZero();
      try (var files = Files.list(stagingPath)) {
        assertThat(files.toList()).isEmpty();
      }
    }
  }

  private static CompletableFuture<Void> consume(
      Flow.Publisher<KafkaTopicCompactedObjectRow> publisher) {
    CompletableFuture<Void> completion = new CompletableFuture<>();
    publisher.subscribe(
        new Flow.Subscriber<>() {
          @Override
          public void onSubscribe(Flow.Subscription subscription) {
            subscription.request(Long.MAX_VALUE);
          }

          @Override
          public void onNext(KafkaTopicCompactedObjectRow item) {}

          @Override
          public void onError(Throwable failure) {
            completion.completeExceptionally(failure);
          }

          @Override
          public void onComplete() {
            completion.complete(null);
          }
        });
    return completion;
  }

  private static KafkaTopicCompactedObjectRow row(long offset, String payloadText) {
    byte[] payload = payloadText.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    return new KafkaTopicCompactedObjectRow(
        offset,
        1,
        KafkaCompactionDispositionV2.RETAIN_VALUE,
        KafkaCompactionKeyEncodingV2.keyed(ByteBuffer.allocate(0)),
        ByteBuffer.wrap(payload),
        Crc32cChecksums.intValue(Crc32cChecksums.checksum(payload)),
        offset,
        0,
        new Checksum(ChecksumType.SHA256, "a".repeat(64)),
        OptionalLong.empty());
  }

  private Path stagingDirectory(String name) throws Exception {
    Path path = Files.createDirectory(temporaryDirectory.resolve(name));
    Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwx------"));
    return path;
  }

  private static StagingFileManager staging(Path path) {
    return new StagingFileManager(
        path,
        32L << 20,
        StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
        Duration.ofHours(1),
        Runnable::run);
  }
}
