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
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.LogConfigHistoryEntry;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.RollReason;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.SegmentState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.VirtualSegment;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class KafkaCompactionPlannerTest {
  private final KafkaCompactionPlanner planner = new KafkaCompactionPlanner();

  @Test
  void selectsOnlyWholeClosedSegmentsAtOrBelowTheLastStableOffset() {
    KafkaCompactionPlanner.Candidate candidate =
        planner.select(snapshot(config(0, 10_000, compactFlags()), 0, 25, Optional.empty()));

    assertThat(candidate.shouldCompact()).isTrue();
    assertThat(candidate.outputCoverage()).isEqualTo(new OffsetRange(0, 20));
    assertThat(candidate.decisionHorizon()).isEqualTo(new OffsetRange(0, 40));
    assertThat(candidate.selectedSegmentCount()).isEqualTo(2);
  }

  @Test
  void usesTheStockStrictMinimumLagPredicateAndStopsAtTheFirstYoungSegment() {
    LogConfigHistoryEntry config = config(100, 10_000, compactFlags());
    KafkaCompactionPlanner.Candidate equality =
        planner.select(snapshot(config, 900, 30, Optional.empty()));
    KafkaCompactionPlanner.Candidate younger =
        planner.select(snapshot(config, 901, 30, Optional.empty()));

    assertThat(equality.outputCoverage()).isEqualTo(new OffsetRange(0, 30));
    assertThat(younger.outputCoverage()).isEqualTo(new OffsetRange(0, 10));
  }

  @Test
  void resumesExactlyAtTheCurrentMandatoryCoverageEnd() {
    LogConfigHistoryEntry config = config(0, 10_000, compactFlags());
    KafkaCompactionPlanner.MandatoryCoverage coverage =
        new KafkaCompactionPlanner.MandatoryCoverage(0, 10, 3, sha256('a'), sha256('b'));

    KafkaCompactionPlanner.Candidate candidate =
        planner.select(snapshot(config, 0, 25, Optional.of(coverage)));

    assertThat(candidate.outputCoverage()).isEqualTo(new OffsetRange(10, 20));
    assertThat(candidate.decisionHorizon()).isEqualTo(new OffsetRange(10, 40));
    assertThat(candidate.previousMandatoryCoverage()).contains(coverage);
  }

  @Test
  void neverSelectsTheActiveSegmentEvenWithNoLagAndStableLso() {
    KafkaCompactionPlanner.Candidate candidate =
        planner.select(snapshot(config(0, 10_000, compactFlags()), 0, 40, Optional.empty()));

    assertThat(candidate.outputCoverage()).isEqualTo(new OffsetRange(0, 30));
    assertThat(candidate.selectedSegmentCount()).isEqualTo(3);
  }

  @Test
  void deleteOnlyPolicyProducesNoCompactionCandidate() {
    KafkaCompactionPlanner.Candidate candidate =
        planner.select(
            snapshot(
                config(0, 10_000, LogConfigHistoryEntry.CLEANUP_DELETE_FLAG),
                0,
                40,
                Optional.empty()));

    assertThat(candidate.shouldCompact()).isFalse();
    assertThat(candidate.outputCoverage()).isEqualTo(new OffsetRange(0, 0));
    assertThat(candidate.decisionHorizon()).isEqualTo(new OffsetRange(0, 40));
  }

  @Test
  void rejectsPolicyOrMandatoryCoverageThatIsNotTheExactCurrentView() {
    LogConfigHistoryEntry config = config(0, 10_000, compactFlags());
    KafkaCompactionPlanner.Policy changed =
        new KafkaCompactionPlanner.Policy(
            config.metadataOffset(),
            config.configDigest(),
            1,
            config.maxCompactionLagMs(),
            config.deleteRetentionMs(),
            config.cleanupPolicyFlags());

    assertThatThrownBy(
            () ->
                new KafkaCompactionPlanner.Snapshot(
                    state(config, 0), changed, Optional.empty(), 30, 30, 1_000))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("config history");

    KafkaCompactionPlanner.MandatoryCoverage wrongStart =
        new KafkaCompactionPlanner.MandatoryCoverage(1, 10, 1, sha256('a'), sha256('b'));
    assertThatThrownBy(() -> snapshot(config, 0, 30, Optional.of(wrongStart)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("retained bounds");
  }

  private static KafkaCompactionPlanner.Snapshot snapshot(
      LogConfigHistoryEntry config,
      long secondSegmentTimestamp,
      long lastStableOffset,
      Optional<KafkaCompactionPlanner.MandatoryCoverage> coverage) {
    return new KafkaCompactionPlanner.Snapshot(
        state(config, secondSegmentTimestamp),
        KafkaCompactionPlanner.Policy.from(config),
        coverage,
        lastStableOffset,
        Math.max(lastStableOffset, 30),
        1_000);
  }

  private static KafkaVirtualSegmentState state(
      LogConfigHistoryEntry config, long secondSegmentTimestamp) {
    return new KafkaVirtualSegmentState(
        0,
        40,
        List.of(
            segment(config, 0, 10, 0, 100, 200, 100, 0, 100, SegmentState.CLOSED),
            segment(
                config, 10, 20, 1, 200, 300, secondSegmentTimestamp, 100, 200, SegmentState.CLOSED),
            segment(config, 20, 30, 2, 300, 400, 500, 200, 300, SegmentState.CLOSED),
            segment(config, 30, 40, 3, 400, 0, 1_000, 300, 400, SegmentState.ACTIVE)),
        List.of(config));
  }

  private static VirtualSegment segment(
      LogConfigHistoryEntry config,
      long baseOffset,
      long endOffset,
      long rollSequence,
      long createdAt,
      long closedAt,
      long largestTimestamp,
      long firstCumulativeBytes,
      long lastCumulativeBytes,
      SegmentState state) {
    return new VirtualSegment(
        baseOffset,
        endOffset,
        rollSequence,
        createdAt,
        closedAt,
        0,
        largestTimestamp,
        endOffset - 1,
        lastCumulativeBytes - firstCumulativeBytes,
        firstCumulativeBytes,
        lastCumulativeBytes,
        config.configDigest(),
        rollSequence == 0 ? RollReason.INITIAL : RollReason.SIZE,
        state);
  }

  private static LogConfigHistoryEntry config(
      long minCompactionLagMs, long maxCompactionLagMs, int cleanupPolicyFlags) {
    return LogConfigHistoryEntry.create(
        7,
        0,
        1_024,
        60_000,
        0,
        1_024,
        64,
        -1,
        -1,
        0,
        86_400_000,
        minCompactionLagMs,
        maxCompactionLagMs,
        0.5,
        cleanupPolicyFlags);
  }

  private static int compactFlags() {
    return LogConfigHistoryEntry.CLEANUP_COMPACT_FLAG | LogConfigHistoryEntry.CLEANUP_DELETE_FLAG;
  }

  private static Checksum sha256(char value) {
    return new Checksum(ChecksumType.SHA256, Character.toString(value).repeat(64));
  }
}
