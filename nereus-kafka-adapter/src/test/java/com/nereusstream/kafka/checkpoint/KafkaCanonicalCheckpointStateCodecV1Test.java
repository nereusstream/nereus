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

package com.nereusstream.kafka.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.kafka.checkpoint.KafkaDerivedIndexState.LogicalByteSample;
import com.nereusstream.kafka.checkpoint.KafkaDerivedIndexState.SegmentLogicalByteIndex;
import com.nereusstream.kafka.checkpoint.KafkaDerivedIndexState.SegmentTimeIndex;
import com.nereusstream.kafka.checkpoint.KafkaDerivedIndexState.TimeIndexEntry;
import com.nereusstream.kafka.checkpoint.KafkaLeaderEpochState.LeaderEpochRange;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.LogConfigHistoryEntry;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.RollReason;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.SegmentState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.VirtualSegment;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSection;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;

class KafkaCanonicalCheckpointStateCodecV1Test {
  private final KafkaCanonicalCheckpointStateCodecV1 codec =
      new KafkaCanonicalCheckpointStateCodecV1();

  @Test
  void composesAllSevenSectionsInCanonicalOrderWithFrozenBytes() throws Exception {
    KafkaCanonicalCheckpointState state = canonicalState();

    List<KafkaCheckpointSection> sections = codec.encodeSections(state);
    KafkaCanonicalCheckpointState decoded = codec.decodeSections(sections, 30, 5, 30);

    assertThat(decoded).isEqualTo(state);
    assertThat(sections)
        .extracting(KafkaCheckpointSection::sectionType)
        .containsExactly(1, 2, 3, 4, 5, 6, 7);
    assertThat(sha256(sections))
        .isEqualTo("f6ee5254e80d21c39c64e3c4a4827eeebbad94337c60c0ad1c71416de5811862");
  }

  @Test
  void decodesOrderIndependentlyAndIgnoresOuterVerifiedOptionalSections() {
    KafkaCanonicalCheckpointState state = canonicalState();
    ArrayList<KafkaCheckpointSection> reordered = new ArrayList<>(codec.encodeSections(state));
    reordered.add(new KafkaCheckpointSection(100, 1, 0, new byte[] {1, 2, 3}));
    Collections.reverse(reordered);

    assertThat(codec.decodeSections(reordered, 30, 5, 30)).isEqualTo(state);
  }

  @Test
  void roundTripsEmptyGenesisAcrossAllSevenRequiredSections() {
    KafkaCanonicalCheckpointState genesis =
        new KafkaCanonicalCheckpointState(
            0,
            0,
            0,
            new KafkaProducerTransactionState(0, List.of(), List.of(), List.of()),
            new KafkaLeaderEpochState(0, 0, List.of()),
            new KafkaVirtualSegmentState(0, 0, List.of(), List.of()),
            new KafkaDerivedIndexState(0, 0, List.of(), List.of()));

    List<KafkaCheckpointSection> sections = codec.encodeSections(genesis);

    assertThat(sections).hasSize(7);
    assertThat(codec.decodeSections(sections, 0, 0, 0)).isEqualTo(genesis);
  }

  @Test
  void rejectsLogicalSegmentSetAndByteFactMismatches() {
    KafkaCanonicalCheckpointState exact = canonicalState();
    KafkaDerivedIndexState mismatchedBytes =
        new KafkaDerivedIndexState(
            5,
            30,
            exact.derivedIndexState().timeIndexes(),
            List.of(
                new SegmentLogicalByteIndex(0, 101, List.of()),
                new SegmentLogicalByteIndex(20, 50, List.of())));

    assertThatThrownBy(
            () ->
                new KafkaCanonicalCheckpointState(
                    30,
                    5,
                    30,
                    exact.producerTransactionState(),
                    exact.leaderEpochState(),
                    exact.virtualSegmentState(),
                    mismatchedBytes))
        .hasMessageContaining("facts differ");

    KafkaDerivedIndexState missingSegment =
        new KafkaDerivedIndexState(
            5, 30, List.of(), List.of(new SegmentLogicalByteIndex(0, 100, List.of())));
    assertThatThrownBy(
            () ->
                new KafkaCanonicalCheckpointState(
                    30,
                    5,
                    30,
                    exact.producerTransactionState(),
                    exact.leaderEpochState(),
                    exact.virtualSegmentState(),
                    missingSegment))
        .hasMessageContaining("sets differ");
  }

  @Test
  void rejectsIndexEntriesThatCrossOrExceedTheirVirtualSegment() {
    KafkaCanonicalCheckpointState exact = canonicalState();
    KafkaDerivedIndexState crossingSample =
        new KafkaDerivedIndexState(
            5,
            30,
            List.of(),
            List.of(
                new SegmentLogicalByteIndex(0, 100, List.of(new LogicalByteSample(25, 20))),
                new SegmentLogicalByteIndex(20, 50, List.of())));
    assertThatThrownBy(() -> full(exact, crossingSample)).hasMessageContaining("sample crosses");

    KafkaDerivedIndexState crossingTime =
        new KafkaDerivedIndexState(
            5,
            30,
            List.of(new SegmentTimeIndex(0, List.of(new TimeIndexEntry(1_000, 25)))),
            exact.derivedIndexState().logicalByteIndexes());
    assertThatThrownBy(() -> full(exact, crossingTime)).hasMessageContaining("time-index");

    KafkaDerivedIndexState excessiveTimestamp =
        new KafkaDerivedIndexState(
            5,
            30,
            List.of(new SegmentTimeIndex(0, List.of(new TimeIndexEntry(1_501, 10)))),
            exact.derivedIndexState().logicalByteIndexes());
    assertThatThrownBy(() -> full(exact, excessiveTimestamp)).hasMessageContaining("time-index");
  }

  @Test
  void rejectsNonStableCheckpointAndMissingRequiredSection() {
    KafkaCanonicalCheckpointState exact = canonicalState();
    assertThatThrownBy(
            () ->
                new KafkaCanonicalCheckpointState(
                    29,
                    5,
                    30,
                    exact.producerTransactionState(),
                    exact.leaderEpochState(),
                    exact.virtualSegmentState(),
                    exact.derivedIndexState()))
        .hasMessageContaining("exact stable end");

    List<KafkaCheckpointSection> sections = codec.encodeSections(exact);
    assertThatThrownBy(() -> codec.decodeSections(sections.subList(0, 6), 30, 5, 30))
        .hasMessageContaining("missing");
    assertThatThrownBy(() -> codec.decodeSections(sections, 29, 5, 30))
        .hasMessageContaining("bounds");
  }

  private static KafkaCanonicalCheckpointState canonicalState() {
    LogConfigHistoryEntry first = config(10, 0, 1_024, 0.5);
    LogConfigHistoryEntry second = config(20, 20, 2_048, 0.6);
    KafkaVirtualSegmentState segments =
        new KafkaVirtualSegmentState(
            5,
            30,
            List.of(
                new VirtualSegment(
                    0,
                    20,
                    3,
                    1_000,
                    2_000,
                    50,
                    1_500,
                    10,
                    100,
                    0,
                    100,
                    first.configDigest(),
                    RollReason.INITIAL,
                    SegmentState.CLOSED),
                new VirtualSegment(
                    20,
                    30,
                    4,
                    2_000,
                    0,
                    75,
                    2_500,
                    25,
                    50,
                    100,
                    150,
                    second.configDigest(),
                    RollReason.SIZE,
                    SegmentState.ACTIVE)),
            List.of(first, second));
    KafkaDerivedIndexState indexes =
        new KafkaDerivedIndexState(
            5,
            30,
            List.of(
                new SegmentTimeIndex(
                    0, List.of(new TimeIndexEntry(1_000, 5), new TimeIndexEntry(1_400, 10))),
                new SegmentTimeIndex(
                    20, List.of(new TimeIndexEntry(2_000, 20), new TimeIndexEntry(2_400, 25)))),
            List.of(
                new SegmentLogicalByteIndex(
                    0, 100, List.of(new LogicalByteSample(5, 20), new LogicalByteSample(10, 40))),
                new SegmentLogicalByteIndex(
                    20, 50, List.of(new LogicalByteSample(20, 0), new LogicalByteSample(25, 25)))));
    return new KafkaCanonicalCheckpointState(
        30,
        5,
        30,
        new KafkaProducerTransactionState(30, List.of(), List.of(), List.of()),
        new KafkaLeaderEpochState(
            5, 30, List.of(new LeaderEpochRange(1, 0), new LeaderEpochRange(2, 20))),
        segments,
        indexes);
  }

  private static KafkaCanonicalCheckpointState full(
      KafkaCanonicalCheckpointState source, KafkaDerivedIndexState indexes) {
    return new KafkaCanonicalCheckpointState(
        source.checkpointOffset(),
        source.logStartOffset(),
        source.stableEndOffset(),
        source.producerTransactionState(),
        source.leaderEpochState(),
        source.virtualSegmentState(),
        indexes);
  }

  private static LogConfigHistoryEntry config(
      long metadataOffset, long effectiveFromOffset, long segmentBytes, double minCleanableRatio) {
    return LogConfigHistoryEntry.create(
        metadataOffset,
        effectiveFromOffset,
        segmentBytes,
        60_000,
        200,
        1_048_576,
        4_096,
        -1,
        604_800_000,
        60_000,
        86_400_000,
        0,
        Long.MAX_VALUE,
        minCleanableRatio,
        LogConfigHistoryEntry.CLEANUP_DELETE_FLAG | LogConfigHistoryEntry.CLEANUP_COMPACT_FLAG);
  }

  private static String sha256(List<KafkaCheckpointSection> sections) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    for (KafkaCheckpointSection section : sections) {
      ByteBuffer header = ByteBuffer.allocate(Integer.BYTES * 3);
      header.putInt(section.sectionType());
      header.putInt(section.sectionVersion());
      header.putInt(section.sectionFlags());
      digest.update(header.array());
      digest.update(section.payload());
    }
    return HexFormat.of().formatHex(digest.digest());
  }
}
