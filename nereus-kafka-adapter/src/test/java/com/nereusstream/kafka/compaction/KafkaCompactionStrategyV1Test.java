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
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.Decision.DROP_EXPIRED_CONTROL;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.Decision.DROP_EXPIRED_TOMBSTONE;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.Decision.DROP_SUPERSEDED;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.Decision.RETAIN_CONTROL;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.Decision.RETAIN_LATEST_VALUE;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.Decision.RETAIN_TOMBSTONE;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.Decision.DROP_UNKEYED;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.MarkerStatus.DELETE_ELIGIBLE;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.MarkerStatus.NOT_CONTROL;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.MarkerStatus.RETAIN_REQUIRED;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.TransactionStatus.ABORTED;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.TransactionStatus.COMMITTED;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.TransactionStatus.DECIDED;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.TransactionStatus.NON_TRANSACTIONAL;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.TransactionStatus.OPEN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.materialization.DecodedCompactionRecord;
import com.nereusstream.materialization.DecodedCompactionRecord.ControlKind;
import com.nereusstream.materialization.DecodedCompactionRecord.KeyKind;
import java.nio.ByteBuffer;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class KafkaCompactionStrategyV1Test {
  private final KafkaCompactionStrategyV1 strategy = new KafkaCompactionStrategyV1();

  @Test
  void retainsTheLatestKeyedValueAndDropsOnlyOlderOccurrences() {
    DecodedCompactionRecord old = record(10, KeyKind.KEYED, false, false);
    DecodedCompactionRecord latest = record(12, KeyKind.KEYED, false, false);

    assertThat(strategy.decide(old, context(false, NON_TRANSACTIONAL))).isEqualTo(DROP_SUPERSEDED);
    assertThat(strategy.decide(latest, context(true, NON_TRANSACTIONAL)))
        .isEqualTo(RETAIN_LATEST_VALUE);
  }

  @Test
  void dropsUnkeyedRecordsLikeStockLogCleaner() {
    DecodedCompactionRecord unkeyed = record(20, KeyKind.UNKEYED, false, false);

    assertThat(strategy.decide(unkeyed, context(true, NON_TRANSACTIONAL)))
        .isEqualTo(DROP_UNKEYED);
  }

  @Test
  void dropsAbortedTransactionalDataAndRejectsAnOpenTransactionCrossingCoverage() {
    DecodedCompactionRecord transactional = record(30, KeyKind.KEYED, false, true);

    assertThat(strategy.decide(transactional, context(true, ABORTED))).isEqualTo(DROP_ABORTED);
    assertThatThrownBy(() -> strategy.decide(transactional, context(true, OPEN)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("open Kafka transaction");
    assertThat(strategy.decide(transactional, context(true, COMMITTED)))
        .isEqualTo(RETAIN_LATEST_VALUE);
  }

  @Test
  void expiresATombstoneAtTheExactDeleteHorizonOnlyAfterAFullScanProof() {
    DecodedCompactionRecord tombstone = record(40, KeyKind.KEYED, true, false);

    assertThat(strategy.decide(tombstone, context(true, NON_TRANSACTIONAL, 100, true, 99)))
        .isEqualTo(RETAIN_TOMBSTONE);
    assertThat(strategy.decide(tombstone, context(true, NON_TRANSACTIONAL, 100, true, 100)))
        .isEqualTo(DROP_EXPIRED_TOMBSTONE);
    assertThat(strategy.decide(tombstone, context(true, NON_TRANSACTIONAL, 100, true, false, 100)))
        .isEqualTo(RETAIN_TOMBSTONE);
    assertThat(strategy.decide(tombstone, context(true, NON_TRANSACTIONAL, 100, false, 1_000)))
        .isEqualTo(RETAIN_TOMBSTONE);
    assertThat(strategy.decide(tombstone, context(true, NON_TRANSACTIONAL)))
        .isEqualTo(RETAIN_TOMBSTONE);
  }

  @Test
  void appliesTheSameFullScanAndHorizonRuleToDecidedControlMarkers() {
    DecodedCompactionRecord control = record(50, KeyKind.CONTROL, false, true);

    assertThat(strategy.decide(control, markerContext(RETAIN_REQUIRED, 100, true, 1_000)))
        .isEqualTo(RETAIN_CONTROL);
    assertThat(strategy.decide(control, markerContext(DELETE_ELIGIBLE, 100, true, 99)))
        .isEqualTo(RETAIN_CONTROL);
    assertThat(strategy.decide(control, markerContext(DELETE_ELIGIBLE, 100, true, 100)))
        .isEqualTo(DROP_EXPIRED_CONTROL);
    assertThat(strategy.decide(control, markerContext(DELETE_ELIGIBLE, 100, false, 1_000)))
        .isEqualTo(RETAIN_CONTROL);
  }

  @Test
  void rejectsMismatchedRecordAndCollectorFacts() {
    DecodedCompactionRecord keyed = record(60, KeyKind.KEYED, false, false);
    DecodedCompactionRecord transactional = record(61, KeyKind.KEYED, false, true);
    DecodedCompactionRecord control = record(62, KeyKind.CONTROL, false, true);

    assertThatThrownBy(() -> strategy.decide(keyed, context(true, COMMITTED)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("non-transactional");
    assertThatThrownBy(() -> strategy.decide(transactional, context(true, NON_TRANSACTIONAL)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("lacks transaction");
    assertThatThrownBy(() -> strategy.decide(keyed, markerContext(RETAIN_REQUIRED, 100, true, 100)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("non-control");
    assertThatThrownBy(() -> strategy.decide(control, context(true, COMMITTED)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("control-marker");
  }

  private static KafkaCompactionStrategyV1.RecordContext context(
      boolean latestForKey, KafkaCompactionStrategyV1.TransactionStatus transactionStatus) {
    return new KafkaCompactionStrategyV1.RecordContext(
        latestForKey, transactionStatus, NOT_CONTROL, OptionalLong.empty(), false, false, 0);
  }

  private static KafkaCompactionStrategyV1.RecordContext context(
      boolean latestForKey,
      KafkaCompactionStrategyV1.TransactionStatus transactionStatus,
      long deleteHorizon,
      boolean fullScanHorizonProven,
      long now) {
    return context(
        latestForKey, transactionStatus, deleteHorizon, fullScanHorizonProven, true, now);
  }

  private static KafkaCompactionStrategyV1.RecordContext context(
      boolean latestForKey,
      KafkaCompactionStrategyV1.TransactionStatus transactionStatus,
      long deleteHorizon,
      boolean fullScanHorizonProven,
      boolean deleteHorizonPreexisting,
      long now) {
    return new KafkaCompactionStrategyV1.RecordContext(
        latestForKey,
        transactionStatus,
        NOT_CONTROL,
        OptionalLong.of(deleteHorizon),
        fullScanHorizonProven,
        deleteHorizonPreexisting,
        now);
  }

  private static KafkaCompactionStrategyV1.RecordContext markerContext(
      KafkaCompactionStrategyV1.MarkerStatus markerStatus,
      long deleteHorizon,
      boolean fullScanHorizonProven,
      long now) {
    return new KafkaCompactionStrategyV1.RecordContext(
        true,
        DECIDED,
        markerStatus,
        OptionalLong.of(deleteHorizon),
        fullScanHorizonProven,
        true,
        now);
  }

  private static DecodedCompactionRecord record(
      long offset, KeyKind keyKind, boolean tombstone, boolean transactional) {
    return new DecodedCompactionRecord(
        offset,
        keyKind,
        keyKind == KeyKind.CONTROL ? ControlKind.ABORT : ControlKind.NONE,
        keyKind == KeyKind.CONTROL ? 4 : -1,
        ByteBuffer.wrap(new byte[] {(byte) keyKind.ordinal(), (byte) offset}),
        tombstone,
        OptionalLong.of(1_000),
        OptionalLong.empty(),
        offset,
        0,
        new Checksum(ChecksumType.SHA256, "a".repeat(64)),
        transactional,
        transactional ? 7 : -1,
        transactional ? (short) 2 : -1,
        keyKind == KeyKind.CONTROL ? -1 : transactional ? 3 : -1,
        ByteBuffer.wrap(new byte[] {1, 2, 3}));
  }
}
