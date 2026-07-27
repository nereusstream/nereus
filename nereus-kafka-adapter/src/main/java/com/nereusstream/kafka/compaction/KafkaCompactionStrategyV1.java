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

import com.nereusstream.materialization.DecodedCompactionRecord;
import com.nereusstream.materialization.DecodedCompactionRecord.KeyKind;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * Deterministic record-decision core fed by transaction and full-scan facts collected over the
 * frozen decision horizon.
 */
public final class KafkaCompactionStrategyV1 {
  public static final String STRATEGY_ID = "kafka-log-cleaner-v1";
  public static final long STRATEGY_VERSION = 1;

  public Decision decide(DecodedCompactionRecord record, RecordContext context) {
    Objects.requireNonNull(record, "record");
    Objects.requireNonNull(context, "context");
    validatePair(record, context);

    if (context.transactionStatus() == TransactionStatus.OPEN) {
      throw new IllegalArgumentException(
          "open Kafka transaction crossed the compaction output coverage");
    }
    if (context.transactionStatus() == TransactionStatus.ABORTED) {
      return Decision.DROP_ABORTED;
    }
    if (record.keyKind() == KeyKind.CONTROL) {
      return controlDecision(context);
    }
    if (record.keyKind() == KeyKind.UNKEYED) {
      return Decision.RETAIN_UNKEYED;
    }
    if (!context.latestForKey()) {
      return Decision.DROP_SUPERSEDED;
    }
    if (!record.tombstone()) {
      return Decision.RETAIN_LATEST_VALUE;
    }
    return horizonPassed(context) ? Decision.DROP_EXPIRED_TOMBSTONE : Decision.RETAIN_TOMBSTONE;
  }

  private static Decision controlDecision(RecordContext context) {
    return switch (context.markerStatus()) {
      case RETAIN_REQUIRED -> Decision.RETAIN_CONTROL;
      case DELETE_ELIGIBLE ->
          horizonPassed(context) ? Decision.DROP_EXPIRED_CONTROL : Decision.RETAIN_CONTROL;
      case NOT_CONTROL ->
          throw new IllegalArgumentException("Kafka control record lacks marker decision facts");
    };
  }

  private static boolean horizonPassed(RecordContext context) {
    return context.fullScanHorizonProven()
        && context.deleteHorizonPreexisting()
        && context.deleteHorizonMillis().isPresent()
        && context.nowMillis() >= context.deleteHorizonMillis().getAsLong();
  }

  private static void validatePair(DecodedCompactionRecord record, RecordContext context) {
    if (record.keyKind() == KeyKind.CONTROL) {
      if (context.markerStatus() == MarkerStatus.NOT_CONTROL
          || !context.latestForKey()
          || context.transactionStatus() != TransactionStatus.DECIDED) {
        throw new IllegalArgumentException("invalid Kafka control-marker compaction facts");
      }
      return;
    }
    if (context.markerStatus() != MarkerStatus.NOT_CONTROL) {
      throw new IllegalArgumentException("non-control Kafka record has marker decision facts");
    }
    validateDataTransactionFacts(record, context);
    if (record.keyKind() == KeyKind.UNKEYED) {
      if (!context.latestForKey()) {
        throw new IllegalArgumentException("invalid unkeyed Kafka compaction facts");
      }
    }
  }

  private static void validateDataTransactionFacts(
      DecodedCompactionRecord record, RecordContext context) {
    if (!record.transactional()
        && context.transactionStatus() != TransactionStatus.NON_TRANSACTIONAL) {
      throw new IllegalArgumentException(
          "non-transactional Kafka record has transactional cleaner facts");
    }
    if (record.transactional()
        && (context.transactionStatus() == TransactionStatus.NON_TRANSACTIONAL
            || context.transactionStatus() == TransactionStatus.DECIDED)) {
      throw new IllegalArgumentException(
          "transactional Kafka record lacks transaction outcome facts");
    }
  }

  public record RecordContext(
      boolean latestForKey,
      TransactionStatus transactionStatus,
      MarkerStatus markerStatus,
      OptionalLong deleteHorizonMillis,
      boolean fullScanHorizonProven,
      boolean deleteHorizonPreexisting,
      long nowMillis) {
    public RecordContext {
      Objects.requireNonNull(transactionStatus, "transactionStatus");
      Objects.requireNonNull(markerStatus, "markerStatus");
      deleteHorizonMillis = Objects.requireNonNull(deleteHorizonMillis, "deleteHorizonMillis");
      if (nowMillis < 0
          || (deleteHorizonMillis.isPresent() && deleteHorizonMillis.getAsLong() < 0)) {
        throw new IllegalArgumentException("invalid Kafka compaction decision context");
      }
    }
  }

  public enum TransactionStatus {
    NON_TRANSACTIONAL,
    COMMITTED,
    ABORTED,
    OPEN,
    DECIDED
  }

  public enum MarkerStatus {
    NOT_CONTROL,
    RETAIN_REQUIRED,
    DELETE_ELIGIBLE
  }

  public enum Decision {
    RETAIN_LATEST_VALUE(true),
    RETAIN_TOMBSTONE(true),
    RETAIN_UNKEYED(true),
    RETAIN_CONTROL(true),
    DROP_SUPERSEDED(false),
    DROP_ABORTED(false),
    DROP_EXPIRED_TOMBSTONE(false),
    DROP_EXPIRED_CONTROL(false);

    private final boolean retained;

    Decision(boolean retained) {
      this.retained = retained;
    }

    public boolean retained() {
      return retained;
    }
  }
}
