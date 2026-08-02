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

package com.nereusstream.kafka.retention;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.LogConfigHistoryEntry;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.RollReason;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.SegmentState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.VirtualSegment;
import java.util.List;
import org.junit.jupiter.api.Test;

class KafkaRetentionPlannerTest {
    private final KafkaRetentionPlanner planner = new KafkaRetentionPlanner();

    @Test
    void combinesStockTimeAndLogicalBytePrefixesAtTheFarthestBoundary() {
        KafkaRetentionPlanner.Snapshot snapshot = snapshot(
                250,
                2_500,
                LogConfigHistoryEntry.CLEANUP_DELETE_FLAG | LogConfigHistoryEntry.CLEANUP_COMPACT_FLAG,
                20,
                5_000);

        KafkaRetentionPlanner.Plan plan = planner.plan(snapshot);

        assertThat(plan.shouldTrim()).isTrue();
        assertThat(plan.previousLogStartOffset()).isZero();
        assertThat(plan.candidateLogStartOffset()).isEqualTo(20);
        assertThat(plan.selectedSegmentCount()).isEqualTo(2);
        assertThat(plan.deletedLogicalBytes()).isEqualTo(200);
        assertThat(plan.totalLogicalBytes()).isEqualTo(400);
        assertThat(plan.timePrefixCount()).isEqualTo(2);
        assertThat(plan.sizePrefixCount()).isEqualTo(1);
        assertThat(plan.reasons())
                .containsExactly(KafkaRetentionPlanner.Reason.TIME, KafkaRetentionPlanner.Reason.SIZE);
        assertThat(plan.trimReason())
                .startsWith("KAFKA_RETENTION_V1:TIME+SIZE:config=7/")
                .contains(":from=0:to=20:segments=2:bytes=200:now=5000");
    }

    @Test
    void usesTheStockStrictTimePredicateAndStopsAtTheFirstIneligibleSegment() {
        KafkaRetentionPlanner.Plan plan =
                planner.plan(snapshot(-1, 2_500, LogConfigHistoryEntry.CLEANUP_DELETE_FLAG, 40, 3_500));

        assertThat(plan.shouldTrim()).isFalse();
        assertThat(plan.candidateLogStartOffset()).isZero();
        assertThat(plan.timePrefixCount()).isZero();
    }

    @Test
    void deletesOnlyAConsecutiveSizePrefixThatFitsTheExcess() {
        KafkaRetentionPlanner.Plan plan =
                planner.plan(snapshot(200, -1, LogConfigHistoryEntry.CLEANUP_DELETE_FLAG, 40, 5_000));

        assertThat(plan.candidateLogStartOffset()).isEqualTo(20);
        assertThat(plan.sizePrefixCount()).isEqualTo(2);
        assertThat(plan.deletedLogicalBytes()).isEqualTo(200);
        assertThat(plan.reasons()).containsExactly(KafkaRetentionPlanner.Reason.SIZE);
    }

    @Test
    void neverSelectsTheActiveSegmentEvenWhenRetentionBytesIsZero() {
        KafkaRetentionPlanner.Plan plan =
                planner.plan(snapshot(0, -1, LogConfigHistoryEntry.CLEANUP_DELETE_FLAG, 40, 5_000));

        assertThat(plan.candidateLogStartOffset()).isEqualTo(30);
        assertThat(plan.selectedSegmentCount()).isEqualTo(3);
        assertThat(plan.deletedLogicalBytes()).isEqualTo(300);
    }

    @Test
    void capsSelectionAtHighWatermarkSegmentBoundaries() {
        KafkaRetentionPlanner.Plan plan =
                planner.plan(snapshot(-1, 0, LogConfigHistoryEntry.CLEANUP_DELETE_FLAG, 15, 5_000));

        assertThat(plan.candidateLogStartOffset()).isEqualTo(10);
        assertThat(plan.selectedSegmentCount()).isEqualTo(1);
    }

    @Test
    void compactOnlyPolicyDoesNotScheduleDeletionRetention() {
        KafkaRetentionPlanner.Plan plan =
                planner.plan(snapshot(0, 0, LogConfigHistoryEntry.CLEANUP_COMPACT_FLAG, 40, 5_000));

        assertThat(plan.shouldTrim()).isFalse();
        assertThat(plan.selectedSegmentCount()).isZero();
    }

    @Test
    void rejectsAPlannerPolicyThatIsNotTheCurrentCheckpointConfig() {
        KafkaRetentionPlanner.Snapshot exact = snapshot(200, -1, LogConfigHistoryEntry.CLEANUP_DELETE_FLAG, 40, 5_000);
        KafkaRetentionPlanner.Policy wrong = new KafkaRetentionPlanner.Policy(
                8,
                exact.policy().configDigest(),
                exact.policy().retentionBytes(),
                exact.policy().retentionMs(),
                exact.policy().cleanupPolicyFlags());

        assertThatThrownBy(() -> new KafkaRetentionPlanner.Snapshot(
                        exact.virtualSegments(),
                        wrong,
                        exact.lastStableOffset(),
                        exact.highWatermark(),
                        exact.nowMillis()))
                .hasMessageContaining("current config history");
    }

    private static KafkaRetentionPlanner.Snapshot snapshot(
            long retentionBytes, long retentionMs, int cleanupPolicyFlags, long highWatermark, long nowMillis) {
        LogConfigHistoryEntry config = LogConfigHistoryEntry.create(
                7,
                0,
                1_024,
                60_000,
                0,
                1_024,
                64,
                retentionBytes,
                retentionMs,
                0,
                86_400_000,
                0,
                Long.MAX_VALUE,
                0.5,
                cleanupPolicyFlags);
        KafkaVirtualSegmentState state = new KafkaVirtualSegmentState(
                0,
                40,
                List.of(
                        segment(config, 0, 10, 0, 100, 200, 1_000, 0, 100, SegmentState.CLOSED),
                        segment(config, 10, 20, 1, 200, 300, 2_000, 100, 200, SegmentState.CLOSED),
                        segment(config, 20, 30, 2, 300, 400, 10_000, 200, 300, SegmentState.CLOSED),
                        segment(config, 30, 40, 3, 400, 0, 11_000, 300, 400, SegmentState.ACTIVE)),
                List.of(config));
        return new KafkaRetentionPlanner.Snapshot(
                state, KafkaRetentionPlanner.Policy.from(config), 0, highWatermark, nowMillis);
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
}
