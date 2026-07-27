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

import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.Decision.DROP_SUPERSEDED;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.Decision.RETAIN_LATEST_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.Snapshot;
import com.nereusstream.materialization.DecodedCompactionRecord;
import com.nereusstream.materialization.DecodedCompactionRecord.ControlKind;
import com.nereusstream.materialization.DecodedCompactionRecord.KeyKind;
import com.nereusstream.objectstore.compacted.KafkaCompactionKeyEncodingV2;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaCompactionWinnerIndexTest {
  @TempDir Path temporaryDirectory;

  private final KafkaCompactionStrategyV1 strategy = new KafkaCompactionStrategyV1();

  @Test
  void spillAndRestartMatchTheUnspilledWinnerProofAcrossTheDecisionTail() throws Exception {
    java.util.ArrayList<DecodedCompactionRecord> records = new java.util.ArrayList<>();
    for (int offset = 0; offset < 40; offset++) {
      records.add(data(offset, offset % 7 == 6 ? "" : "k" + offset % 7));
    }
    Snapshot inMemorySnapshot = snapshot(1 << 20);
    KafkaCompactionPassOneCollector reference =
        new KafkaCompactionPassOneCollector(inMemorySnapshot);
    records.forEach(reference::accept);
    KafkaCompactionPassOneCollector.Facts expected = reference.finish();

    Path stagingPath = stagingDirectory("spill-restart");
    try (StagingFileManager staging = staging(stagingPath)) {
      KafkaCompactionPassOneCollector.Facts first = collect(snapshot(1), staging, records);
      KafkaCompactionPassOneCollector.Facts restarted = collect(snapshot(1), staging, records);

      assertThat(first.fullFactSha256()).isEqualTo(expected.fullFactSha256());
      assertThat(first.outputFactSha256()).isEqualTo(expected.outputFactSha256());
      assertThat(restarted.fullFactSha256()).isEqualTo(first.fullFactSha256());
      assertThat(restarted.outputFactSha256()).isEqualTo(first.outputFactSha256());
      assertThat(first.spillRunCount()).isGreaterThan(records.size());
      assertThat(restarted.spillRunCount()).isEqualTo(first.spillRunCount());
      assertThat(first.peakInMemoryKeyBytes()).isLessThanOrEqualTo(67);
      assertThat(staging.reservedBytes()).isZero();
      try (var files = Files.list(stagingPath)) {
        assertThat(files.toList()).isEmpty();
      }

      for (int offset = 0; offset < 35; offset++) {
        assertThat(strategy.decide(records.get(offset), first.contextFor(records.get(offset))))
            .isEqualTo(
                strategy.decide(records.get(offset), expected.contextFor(records.get(offset))));
      }
      assertDecision(first, records.get(28), DROP_SUPERSEDED);
      assertDecision(first, records.get(34), RETAIN_LATEST_VALUE);
    }
  }

  @Test
  void rejectsCorruptedSealedRunAndReleasesEveryStagingPermit() throws Exception {
    Path stagingPath = stagingDirectory("corrupt");
    try (StagingFileManager staging = staging(stagingPath);
        KafkaCompactionPassOneCollector collector =
            new KafkaCompactionPassOneCollector(singleRecordSnapshot(), staging)) {
      collector.accept(data(0, "a"));
      Path run;
      try (var files = Files.list(stagingPath)) {
        run = files.findFirst().orElseThrow();
      }
      try (FileChannel channel = FileChannel.open(run, StandardOpenOption.WRITE)) {
        channel.write(ByteBuffer.wrap(new byte[] {(byte) 'z'}), 13);
        channel.force(true);
      }

      assertThatThrownBy(collector::finish)
          .isInstanceOf(NereusException.class)
          .hasMessageContaining("read Kafka compaction spill run");
      assertThat(staging.reservedBytes()).isZero();
      assertThat(Files.exists(run)).isFalse();
    }
  }

  @Test
  void cancellationDeletesUnfinishedRunsAndReleasesTheGlobalBudget() throws Exception {
    Path stagingPath = stagingDirectory("cancel");
    try (StagingFileManager staging = staging(stagingPath)) {
      KafkaCompactionPassOneCollector collector =
          new KafkaCompactionPassOneCollector(snapshot(1), staging);
      collector.accept(data(0, "a"));
      assertThat(staging.reservedBytes()).isPositive();

      collector.close();

      assertThat(staging.reservedBytes()).isZero();
      try (var files = Files.list(stagingPath)) {
        assertThat(files.toList()).isEmpty();
      }
    }
  }

  private KafkaCompactionPassOneCollector.Facts collect(
      Snapshot snapshot, StagingFileManager staging, List<DecodedCompactionRecord> records) {
    try (KafkaCompactionPassOneCollector collector =
        new KafkaCompactionPassOneCollector(snapshot, staging)) {
      records.forEach(collector::accept);
      return collector.finish();
    }
  }

  private void assertDecision(
      KafkaCompactionPassOneCollector.Facts facts,
      DecodedCompactionRecord record,
      KafkaCompactionStrategyV1.Decision decision) {
    assertThat(strategy.decide(record, facts.contextFor(record))).isEqualTo(decision);
  }

  private Snapshot snapshot(long maxInMemoryKeyBytes) {
    return new Snapshot(
        new OffsetRange(0, 35),
        new OffsetRange(0, 40),
        40,
        100,
        1_000,
        100,
        1 << 20,
        maxInMemoryKeyBytes,
        List.of(),
        List.of(),
        List.of());
  }

  private Snapshot singleRecordSnapshot() {
    return new Snapshot(
        new OffsetRange(0, 1),
        new OffsetRange(0, 1),
        1,
        100,
        1_000,
        1,
        1 << 20,
        1,
        List.of(),
        List.of(),
        List.of());
  }

  private static DecodedCompactionRecord data(long offset, String key) {
    return new DecodedCompactionRecord(
        offset,
        KeyKind.KEYED,
        ControlKind.NONE,
        -1,
        KafkaCompactionKeyEncodingV2.keyed(
            ByteBuffer.wrap(key.getBytes(java.nio.charset.StandardCharsets.UTF_8))),
        false,
        OptionalLong.of(1_000 + offset),
        OptionalLong.empty(),
        offset,
        0,
        new Checksum(ChecksumType.SHA256, "a".repeat(64)),
        false,
        -1,
        (short) -1,
        -1,
        ByteBuffer.wrap(new byte[] {1, 2, (byte) offset}));
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
