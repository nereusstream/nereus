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

import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.Decision.DROP_ABORTED;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.Decision.DROP_EXPIRED_TOMBSTONE;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.Decision.DROP_SUPERSEDED;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.Decision.DROP_UNKEYED;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.Decision.RETAIN_CONTROL;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.Decision.RETAIN_LATEST_VALUE;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.Decision.RETAIN_TOMBSTONE;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.MarkerStatus.RETAIN_REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.AbortedTransactionRange;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.MarkerDecision;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.OpenTransactionRange;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.PassTwoVerifier;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.Snapshot;
import com.nereusstream.materialization.DecodedCompactionRecord;
import com.nereusstream.materialization.DecodedCompactionRecord.ControlKind;
import com.nereusstream.materialization.DecodedCompactionRecord.KeyKind;
import com.nereusstream.objectstore.compacted.KafkaCompactionKeyEncodingV2;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class KafkaCompactionPassOneCollectorTest {
    private final KafkaCompactionStrategyV1 strategy = new KafkaCompactionStrategyV1();

    @Test
    void usesTheEntireFrozenHorizonForWinnersAndReprovesOutputFactsInPassTwo() {
        DecodedCompactionRecord old = data(0, "k", false, false, -1, OptionalLong.empty());
        DecodedCompactionRecord unkeyed = unkeyed(1);
        DecodedCompactionRecord tailWinner = data(2, "k", false, false, -1, OptionalLong.empty());
        KafkaCompactionPassOneCollector collector =
                new KafkaCompactionPassOneCollector(snapshot(0, 2, 3, List.of(), List.of(), List.of()));
        collector.accept(old);
        collector.accept(unkeyed);
        collector.accept(tailWinner);

        KafkaCompactionPassOneCollector.Facts facts = collector.finish();

        assertThat(strategy.decide(old, facts.contextFor(old))).isEqualTo(DROP_SUPERSEDED);
        assertThat(strategy.decide(unkeyed, facts.contextFor(unkeyed))).isEqualTo(DROP_UNKEYED);
        assertThat(facts.scannedRecordCount()).isEqualTo(3);
        assertThat(facts.outputRecordCount()).isEqualTo(2);
        assertThat(facts.fullFactSha256().type()).isEqualTo(ChecksumType.SHA256);
        assertThat(facts.outputFactSha256()).isNotEqualTo(facts.fullFactSha256());

        PassTwoVerifier verifier = facts.newPassTwoVerifier();
        verifier.accept(old);
        verifier.accept(unkeyed);
        verifier.finish();
    }

    @Test
    void excludesAbortedDataFromTheWinnerMapAndUsesFrozenMarkerFacts() {
        DecodedCompactionRecord committed = data(0, "k", false, false, -1, OptionalLong.empty());
        DecodedCompactionRecord other = data(1, "other", false, false, -1, OptionalLong.empty());
        DecodedCompactionRecord aborted = data(2, "k", false, true, 7, OptionalLong.empty());
        DecodedCompactionRecord marker = abortMarker(3, 7, OptionalLong.empty());
        Snapshot snapshot = snapshot(
                0,
                4,
                4,
                List.of(new AbortedTransactionRange(7, 2, 3)),
                List.of(),
                List.of(new MarkerDecision(3, RETAIN_REQUIRED)));
        KafkaCompactionPassOneCollector collector = new KafkaCompactionPassOneCollector(snapshot);
        collector.accept(committed);
        collector.accept(other);
        collector.accept(aborted);
        collector.accept(marker);

        KafkaCompactionPassOneCollector.Facts facts = collector.finish();

        assertThat(strategy.decide(committed, facts.contextFor(committed))).isEqualTo(RETAIN_LATEST_VALUE);
        assertThat(strategy.decide(aborted, facts.contextFor(aborted))).isEqualTo(DROP_ABORTED);
        assertThat(strategy.decide(marker, facts.contextFor(marker))).isEqualTo(RETAIN_CONTROL);
        assertThat(facts.rewriteDeleteHorizon(marker)).isEmpty();
    }

    @Test
    void ignoresAnOpenTailForWinnersButRejectsAnOpenTransactionInsideOutputCoverage() {
        DecodedCompactionRecord committed = data(0, "k", false, false, -1, OptionalLong.empty());
        DecodedCompactionRecord other = data(1, "other", false, false, -1, OptionalLong.empty());
        DecodedCompactionRecord open = data(2, "k", false, true, 9, OptionalLong.empty());
        Snapshot tailSnapshot = snapshot(0, 2, 3, List.of(), List.of(new OpenTransactionRange(9, 2)), List.of());
        KafkaCompactionPassOneCollector tailCollector = new KafkaCompactionPassOneCollector(tailSnapshot);
        tailCollector.accept(committed);
        tailCollector.accept(other);
        tailCollector.accept(open);

        assertThat(strategy.decide(committed, tailCollector.finish().contextFor(committed)))
                .isEqualTo(RETAIN_LATEST_VALUE);

        KafkaCompactionPassOneCollector crossingCollector = new KafkaCompactionPassOneCollector(
                snapshot(0, 3, 3, List.of(), List.of(new OpenTransactionRange(9, 2)), List.of()));
        crossingCollector.accept(committed);
        crossingCollector.accept(other);
        assertThatThrownBy(() -> crossingCollector.accept(open))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("open Kafka transaction");
    }

    @Test
    void retainsANewDeleteHorizonThenDropsAPreexistingHorizonAtEquality() {
        DecodedCompactionRecord firstPass = data(0, "new", true, false, -1, OptionalLong.empty());
        DecodedCompactionRecord laterPass = data(1, "old", true, false, -1, OptionalLong.of(100));
        KafkaCompactionPassOneCollector collector =
                new KafkaCompactionPassOneCollector(snapshot(0, 2, 2, List.of(), List.of(), List.of()));
        collector.accept(firstPass);
        collector.accept(laterPass);

        KafkaCompactionPassOneCollector.Facts facts = collector.finish();

        assertThat(facts.rewriteDeleteHorizon(firstPass)).isEqualTo(OptionalLong.of(1_100));
        assertThat(strategy.decide(firstPass, facts.contextFor(firstPass))).isEqualTo(RETAIN_TOMBSTONE);
        assertThat(strategy.decide(laterPass, facts.contextFor(laterPass))).isEqualTo(DROP_EXPIRED_TOMBSTONE);
    }

    @Test
    void failsClosedOnIncompleteDenseMarkerOrPassTwoFacts() {
        assertThatThrownBy(() -> new Snapshot(
                        new OffsetRange(0, 1),
                        new OffsetRange(0, 2),
                        1,
                        100,
                        1_000,
                        100,
                        1 << 20,
                        1 << 20,
                        List.of(),
                        List.of(),
                        List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("snapshot");

        KafkaCompactionPassOneCollector dense =
                new KafkaCompactionPassOneCollector(snapshot(0, 1, 2, List.of(), List.of(), List.of()));
        assertThatThrownBy(() -> dense.accept(data(1, "k", false, false, -1, OptionalLong.empty())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("densely");

        KafkaCompactionPassOneCollector marker =
                new KafkaCompactionPassOneCollector(snapshot(0, 1, 1, List.of(), List.of(), List.of()));
        assertThatThrownBy(() -> marker.accept(abortMarker(0, 7, OptionalLong.empty())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lacks a frozen");

        DecodedCompactionRecord original = data(0, "k", false, false, -1, OptionalLong.empty());
        KafkaCompactionPassOneCollector passOne =
                new KafkaCompactionPassOneCollector(snapshot(0, 1, 1, List.of(), List.of(), List.of()));
        passOne.accept(original);
        PassTwoVerifier verifier = passOne.finish().newPassTwoVerifier();
        verifier.accept(data(0, "changed", false, false, -1, OptionalLong.empty()));
        assertThatThrownBy(verifier::finish)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("differ");
    }

    private static Snapshot snapshot(
            long start,
            long outputEnd,
            long horizonEnd,
            List<AbortedTransactionRange> aborted,
            List<OpenTransactionRange> open,
            List<MarkerDecision> markers) {
        return new Snapshot(
                new OffsetRange(start, outputEnd),
                new OffsetRange(start, horizonEnd),
                Math.max(4, horizonEnd),
                100,
                1_000,
                100,
                1 << 20,
                1 << 20,
                aborted,
                open,
                markers);
    }

    private static DecodedCompactionRecord data(
            long offset,
            String key,
            boolean tombstone,
            boolean transactional,
            long producerId,
            OptionalLong deleteHorizon) {
        return new DecodedCompactionRecord(
                offset,
                KeyKind.KEYED,
                ControlKind.NONE,
                -1,
                KafkaCompactionKeyEncodingV2.keyed(ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8))),
                tombstone,
                OptionalLong.of(1_000 + offset),
                deleteHorizon,
                offset,
                0,
                new Checksum(ChecksumType.SHA256, "a".repeat(64)),
                transactional,
                transactional ? producerId : -1,
                transactional ? (short) 2 : -1,
                transactional ? Math.toIntExact(offset) : -1,
                ByteBuffer.wrap(new byte[] {1, 2, (byte) offset}));
    }

    private static DecodedCompactionRecord unkeyed(long offset) {
        return new DecodedCompactionRecord(
                offset,
                KeyKind.UNKEYED,
                ControlKind.NONE,
                -1,
                KafkaCompactionKeyEncodingV2.nullKey(offset),
                false,
                OptionalLong.of(1_000 + offset),
                OptionalLong.empty(),
                offset,
                0,
                new Checksum(ChecksumType.SHA256, "b".repeat(64)),
                false,
                -1,
                (short) -1,
                -1,
                ByteBuffer.wrap(new byte[] {3, 4, (byte) offset}));
    }

    private static DecodedCompactionRecord abortMarker(long offset, long producerId, OptionalLong deleteHorizon) {
        return new DecodedCompactionRecord(
                offset,
                KeyKind.CONTROL,
                ControlKind.ABORT,
                4,
                KafkaCompactionKeyEncodingV2.control(offset),
                false,
                OptionalLong.of(1_000 + offset),
                deleteHorizon,
                offset,
                0,
                new Checksum(ChecksumType.SHA256, "c".repeat(64)),
                true,
                producerId,
                (short) 2,
                -1,
                ByteBuffer.wrap(new byte[] {5, 6, (byte) offset}));
    }
}
