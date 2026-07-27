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
import com.nereusstream.api.OffsetRange;
import com.nereusstream.materialization.DecodedCompactionRecord;
import com.nereusstream.materialization.DecodedCompactionRecord.KeyKind;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;

/**
 * In-memory reference pass-one collector for the frozen Kafka decision horizon.
 *
 * <p>The durable worker will replace the winner map with bounded sorted spill, but this class
 * already freezes the transaction, marker, horizon, dense-scan, and pass-two verification
 * semantics.
 */
public final class KafkaCompactionPassOneCollector {
  public static final int MAX_TRANSACTION_FACTS = 65_536;

  private static final byte[] FULL_DIGEST_DOMAIN =
      "NEREUS_KAFKA_COMPACTION_PASS1_FULL_V1\0".getBytes(StandardCharsets.UTF_8);
  private static final byte[] OUTPUT_DIGEST_DOMAIN =
      "NEREUS_KAFKA_COMPACTION_PASS1_OUTPUT_V1\0".getBytes(StandardCharsets.UTF_8);

  private final Snapshot snapshot;
  private final Map<ByteKey, Long> greatestEligibleOffsets = new HashMap<>();
  private final Map<Long, KafkaCompactionStrategyV1.MarkerStatus> markerStatuses;
  private final Set<Long> observedMarkers = new HashSet<>();
  private final RecordFactDigest fullDigest = new RecordFactDigest(FULL_DIGEST_DOMAIN);
  private final RecordFactDigest outputDigest = new RecordFactDigest(OUTPUT_DIGEST_DOMAIN);
  private long nextOffset;
  private long scannedRecords;
  private long outputRecords;
  private long inMemoryKeyBytes;
  private boolean finished;

  public KafkaCompactionPassOneCollector(Snapshot snapshot) {
    this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
    this.nextOffset = snapshot.decisionHorizon().startOffset();
    this.markerStatuses = markerStatusMap(snapshot.markerDecisions());
  }

  public void accept(DecodedCompactionRecord record) {
    Objects.requireNonNull(record, "record");
    if (finished) {
      throw new IllegalStateException("Kafka compaction pass one is already finished");
    }
    if (record.absoluteOffset() != nextOffset
        || !snapshot.decisionHorizon().contains(record.absoluteOffset())) {
      throw new IllegalArgumentException(
          "Kafka compaction pass one must scan the decision horizon densely");
    }
    nextOffset = Math.addExact(nextOffset, 1);
    scannedRecords = Math.addExact(scannedRecords, 1);
    if (scannedRecords > snapshot.maxDecodedRecords()
        || record.taggedCompactionKey().remaining() > snapshot.maxKeyBytes()) {
      throw new IllegalArgumentException(
          "Kafka compaction pass one exceeded its decoded-record or key limit");
    }
    fullDigest.add(record);
    if (snapshot.outputCoverage().contains(record.absoluteOffset())) {
      outputRecords = Math.addExact(outputRecords, 1);
      outputDigest.add(record);
    }

    KafkaCompactionStrategyV1.TransactionStatus transactionStatus =
        transactionStatus(record, snapshot);
    if (transactionStatus == KafkaCompactionStrategyV1.TransactionStatus.OPEN
        && snapshot.outputCoverage().contains(record.absoluteOffset())) {
      throw new IllegalArgumentException(
          "open Kafka transaction crosses the compaction output coverage");
    }
    if (record.keyKind() == KeyKind.KEYED
        && transactionStatus != KafkaCompactionStrategyV1.TransactionStatus.ABORTED
        && transactionStatus != KafkaCompactionStrategyV1.TransactionStatus.OPEN) {
      ByteKey key = new ByteKey(bytes(record.taggedCompactionKey()));
      Long previous = greatestEligibleOffsets.get(key);
      if (previous == null) {
        inMemoryKeyBytes = Math.addExact(inMemoryKeyBytes, Math.addExact(key.length(), 64L));
        if (inMemoryKeyBytes > snapshot.maxInMemoryKeyBytes()) {
          throw new IllegalArgumentException(
              "Kafka compaction pass one exceeded its in-memory key budget");
        }
        greatestEligibleOffsets.put(key, record.absoluteOffset());
      } else if (record.absoluteOffset() > previous) {
        greatestEligibleOffsets.put(key, record.absoluteOffset());
      }
    }
    if (record.keyKind() == KeyKind.CONTROL) {
      if (!markerStatuses.containsKey(record.absoluteOffset())) {
        throw new IllegalArgumentException("Kafka control marker lacks a frozen pass-one decision");
      }
      observedMarkers.add(record.absoluteOffset());
    }
  }

  public Facts finish() {
    if (finished) {
      throw new IllegalStateException("Kafka compaction pass one is already finished");
    }
    finished = true;
    if (nextOffset != snapshot.decisionHorizon().endOffset()) {
      throw new IllegalArgumentException(
          "Kafka compaction pass one ended before the frozen decision horizon");
    }
    if (!observedMarkers.equals(markerStatuses.keySet())) {
      throw new IllegalArgumentException(
          "Kafka compaction marker decisions do not equal the scanned control markers");
    }
    if (scannedRecords != snapshot.decisionHorizon().recordCount()
        || outputRecords != snapshot.outputCoverage().recordCount()) {
      throw new IllegalStateException("Kafka compaction pass-one record accounting changed");
    }
    return new Facts(
        snapshot,
        greatestEligibleOffsets,
        fullDigest.finish(),
        outputDigest.finish(),
        scannedRecords,
        outputRecords,
        markerStatuses);
  }

  public record Snapshot(
      OffsetRange outputCoverage,
      OffsetRange decisionHorizon,
      long transactionStateEndOffset,
      long nowMillis,
      long deleteRetentionMs,
      long maxDecodedRecords,
      int maxKeyBytes,
      long maxInMemoryKeyBytes,
      List<AbortedTransactionRange> abortedTransactions,
      List<OpenTransactionRange> openTransactions,
      List<MarkerDecision> markerDecisions) {
    public Snapshot {
      Objects.requireNonNull(outputCoverage, "outputCoverage");
      Objects.requireNonNull(decisionHorizon, "decisionHorizon");
      abortedTransactions =
          List.copyOf(Objects.requireNonNull(abortedTransactions, "abortedTransactions"));
      openTransactions = List.copyOf(Objects.requireNonNull(openTransactions, "openTransactions"));
      markerDecisions = List.copyOf(Objects.requireNonNull(markerDecisions, "markerDecisions"));
      if (outputCoverage.isEmpty()
          || decisionHorizon.isEmpty()
          || outputCoverage.startOffset() != decisionHorizon.startOffset()
          || outputCoverage.endOffset() > decisionHorizon.endOffset()
          || transactionStateEndOffset < decisionHorizon.endOffset()
          || nowMillis < 0
          || deleteRetentionMs < 0
          || maxDecodedRecords <= 0
          || maxKeyBytes <= 0
          || maxInMemoryKeyBytes <= 0
          || abortedTransactions.size() > MAX_TRANSACTION_FACTS
          || openTransactions.size() > MAX_TRANSACTION_FACTS
          || markerDecisions.size() > MAX_TRANSACTION_FACTS
          || abortedTransactions.size() > maxDecodedRecords
          || openTransactions.size() > maxDecodedRecords
          || markerDecisions.size() > maxDecodedRecords) {
        throw new IllegalArgumentException("invalid Kafka compaction pass-one snapshot");
      }
      Math.addExact(nowMillis, deleteRetentionMs);
      requireCanonicalAborted(abortedTransactions, transactionStateEndOffset);
      requireCanonicalOpen(openTransactions, transactionStateEndOffset);
      requireCanonicalMarkers(markerDecisions, decisionHorizon);
    }
  }

  public record AbortedTransactionRange(long producerId, long firstOffset, long markerOffset) {
    public AbortedTransactionRange {
      if (producerId < 0 || firstOffset < 0 || markerOffset <= firstOffset) {
        throw new IllegalArgumentException("invalid aborted Kafka transaction range");
      }
    }

    boolean containsData(long offset) {
      return offset >= firstOffset && offset < markerOffset;
    }
  }

  public record OpenTransactionRange(long producerId, long firstOffset) {
    public OpenTransactionRange {
      if (producerId < 0 || firstOffset < 0) {
        throw new IllegalArgumentException("invalid open Kafka transaction range");
      }
    }
  }

  public record MarkerDecision(long markerOffset, KafkaCompactionStrategyV1.MarkerStatus status) {
    public MarkerDecision {
      Objects.requireNonNull(status, "status");
      if (markerOffset < 0 || status == KafkaCompactionStrategyV1.MarkerStatus.NOT_CONTROL) {
        throw new IllegalArgumentException("invalid Kafka transaction-marker decision");
      }
    }
  }

  public static final class Facts {
    private final Snapshot snapshot;
    private final Map<ByteKey, Long> greatestEligibleOffsets;
    private final Checksum fullFactSha256;
    private final Checksum outputFactSha256;
    private final long scannedRecordCount;
    private final long outputRecordCount;
    private final Map<Long, KafkaCompactionStrategyV1.MarkerStatus> markerStatuses;

    private Facts(
        Snapshot snapshot,
        Map<ByteKey, Long> greatestEligibleOffsets,
        Checksum fullFactSha256,
        Checksum outputFactSha256,
        long scannedRecordCount,
        long outputRecordCount,
        Map<Long, KafkaCompactionStrategyV1.MarkerStatus> markerStatuses) {
      this.snapshot = snapshot;
      this.greatestEligibleOffsets =
          Map.copyOf(Objects.requireNonNull(greatestEligibleOffsets, "greatestEligibleOffsets"));
      this.fullFactSha256 = Objects.requireNonNull(fullFactSha256, "fullFactSha256");
      this.outputFactSha256 = Objects.requireNonNull(outputFactSha256, "outputFactSha256");
      this.scannedRecordCount = scannedRecordCount;
      this.outputRecordCount = outputRecordCount;
      this.markerStatuses = Map.copyOf(markerStatuses);
    }

    public KafkaCompactionStrategyV1.RecordContext contextFor(DecodedCompactionRecord record) {
      Objects.requireNonNull(record, "record");
      if (!snapshot.outputCoverage().contains(record.absoluteOffset())) {
        throw new IllegalArgumentException(
            "Kafka compaction decision requested outside output coverage");
      }
      KafkaCompactionStrategyV1.TransactionStatus transactionStatus =
          transactionStatus(record, snapshot);
      long greatestOffset =
          switch (record.keyKind()) {
            case UNKEYED, CONTROL -> record.absoluteOffset();
            case KEYED -> {
              if (transactionStatus == KafkaCompactionStrategyV1.TransactionStatus.ABORTED
                  || transactionStatus == KafkaCompactionStrategyV1.TransactionStatus.OPEN) {
                yield record.absoluteOffset();
              }
              Long value =
                  greatestEligibleOffsets.get(new ByteKey(bytes(record.taggedCompactionKey())));
              if (value == null) {
                throw new IllegalArgumentException(
                    "Kafka keyed record has no pass-one winner fact");
              }
              yield value;
            }
          };
      KafkaCompactionStrategyV1.MarkerStatus markerStatus =
          record.keyKind() == KeyKind.CONTROL
              ? markerStatuses.get(record.absoluteOffset())
              : KafkaCompactionStrategyV1.MarkerStatus.NOT_CONTROL;
      if (markerStatus == null) {
        throw new IllegalArgumentException("Kafka control marker lacks a frozen pass-one decision");
      }
      OptionalLong deleteHorizon = effectiveDeleteHorizon(record, snapshot);
      return new KafkaCompactionStrategyV1.RecordContext(
          greatestOffset,
          transactionStatus,
          markerStatus,
          deleteHorizon,
          true,
          record.deleteHorizonMillis().isPresent(),
          snapshot.nowMillis());
    }

    public OptionalLong rewriteDeleteHorizon(DecodedCompactionRecord record) {
      Objects.requireNonNull(record, "record");
      if (!snapshot.outputCoverage().contains(record.absoluteOffset())) {
        throw new IllegalArgumentException(
            "Kafka compaction rewrite requested outside output coverage");
      }
      return effectiveDeleteHorizon(record, snapshot);
    }

    public PassTwoVerifier newPassTwoVerifier() {
      return new PassTwoVerifier(snapshot.outputCoverage(), outputFactSha256, outputRecordCount);
    }

    public Checksum fullFactSha256() {
      return fullFactSha256;
    }

    public Checksum outputFactSha256() {
      return outputFactSha256;
    }

    public long scannedRecordCount() {
      return scannedRecordCount;
    }

    public long outputRecordCount() {
      return outputRecordCount;
    }

    public OffsetRange outputCoverage() {
      return snapshot.outputCoverage();
    }

    public OffsetRange decisionHorizon() {
      return snapshot.decisionHorizon();
    }
  }

  public static final class PassTwoVerifier {
    private final OffsetRange outputCoverage;
    private final Checksum expectedSha256;
    private final long expectedRecords;
    private final RecordFactDigest digest = new RecordFactDigest(OUTPUT_DIGEST_DOMAIN);
    private long nextOffset;
    private long records;
    private boolean finished;

    private PassTwoVerifier(
        OffsetRange outputCoverage, Checksum expectedSha256, long expectedRecords) {
      this.outputCoverage = outputCoverage;
      this.expectedSha256 = expectedSha256;
      this.expectedRecords = expectedRecords;
      this.nextOffset = outputCoverage.startOffset();
    }

    public void accept(DecodedCompactionRecord record) {
      Objects.requireNonNull(record, "record");
      if (finished) {
        throw new IllegalStateException("Kafka compaction pass two is already finished");
      }
      if (record.absoluteOffset() != nextOffset
          || !outputCoverage.contains(record.absoluteOffset())) {
        throw new IllegalArgumentException(
            "Kafka compaction pass two must replay output coverage densely");
      }
      nextOffset = Math.addExact(nextOffset, 1);
      records = Math.addExact(records, 1);
      digest.add(record);
    }

    public void finish() {
      if (finished) {
        throw new IllegalStateException("Kafka compaction pass two is already finished");
      }
      finished = true;
      if (nextOffset != outputCoverage.endOffset()
          || records != expectedRecords
          || !digest.finish().equals(expectedSha256)) {
        throw new IllegalArgumentException("Kafka compaction pass-two facts differ from pass one");
      }
    }
  }

  private static KafkaCompactionStrategyV1.TransactionStatus transactionStatus(
      DecodedCompactionRecord record, Snapshot snapshot) {
    if (record.keyKind() == KeyKind.CONTROL) {
      return KafkaCompactionStrategyV1.TransactionStatus.DECIDED;
    }
    if (!record.transactional()) {
      return KafkaCompactionStrategyV1.TransactionStatus.NON_TRANSACTIONAL;
    }
    for (OpenTransactionRange open : snapshot.openTransactions()) {
      if (open.producerId() == record.producerId()
          && record.absoluteOffset() >= open.firstOffset()) {
        return KafkaCompactionStrategyV1.TransactionStatus.OPEN;
      }
    }
    for (AbortedTransactionRange aborted : snapshot.abortedTransactions()) {
      if (aborted.producerId() == record.producerId()
          && aborted.containsData(record.absoluteOffset())) {
        return KafkaCompactionStrategyV1.TransactionStatus.ABORTED;
      }
    }
    return KafkaCompactionStrategyV1.TransactionStatus.COMMITTED;
  }

  private static OptionalLong effectiveDeleteHorizon(
      DecodedCompactionRecord record, Snapshot snapshot) {
    if (record.deleteHorizonMillis().isPresent()) {
      return record.deleteHorizonMillis();
    }
    if (record.tombstone() || record.keyKind() == KeyKind.CONTROL) {
      return OptionalLong.of(Math.addExact(snapshot.nowMillis(), snapshot.deleteRetentionMs()));
    }
    return OptionalLong.empty();
  }

  private static Map<Long, KafkaCompactionStrategyV1.MarkerStatus> markerStatusMap(
      List<MarkerDecision> decisions) {
    LinkedHashMap<Long, KafkaCompactionStrategyV1.MarkerStatus> result = new LinkedHashMap<>();
    for (MarkerDecision decision : decisions) {
      result.put(decision.markerOffset(), decision.status());
    }
    return Map.copyOf(result);
  }

  private static void requireCanonicalAborted(
      List<AbortedTransactionRange> transactions, long stateEndOffset) {
    long previousFirst = -1;
    long previousProducer = -1;
    Map<Long, Long> previousMarkerByProducer = new HashMap<>();
    for (AbortedTransactionRange transaction : transactions) {
      Objects.requireNonNull(transaction, "abortedTransaction");
      if (transaction.markerOffset() >= stateEndOffset
          || transaction.firstOffset() < previousFirst
          || (transaction.firstOffset() == previousFirst
              && transaction.producerId() <= previousProducer)
          || transaction.firstOffset()
              <= previousMarkerByProducer.getOrDefault(transaction.producerId(), -1L)) {
        throw new IllegalArgumentException(
            "aborted Kafka transaction ranges are not canonical and complete");
      }
      previousFirst = transaction.firstOffset();
      previousProducer = transaction.producerId();
      previousMarkerByProducer.put(transaction.producerId(), transaction.markerOffset());
    }
  }

  private static void requireCanonicalOpen(
      List<OpenTransactionRange> transactions, long stateEndOffset) {
    long previousFirst = -1;
    long previousProducer = -1;
    Set<Long> producers = new HashSet<>();
    for (OpenTransactionRange transaction : transactions) {
      Objects.requireNonNull(transaction, "openTransaction");
      if (transaction.firstOffset() >= stateEndOffset
          || transaction.firstOffset() < previousFirst
          || (transaction.firstOffset() == previousFirst
              && transaction.producerId() <= previousProducer)
          || !producers.add(transaction.producerId())) {
        throw new IllegalArgumentException(
            "open Kafka transaction ranges are not canonical and complete");
      }
      previousFirst = transaction.firstOffset();
      previousProducer = transaction.producerId();
    }
  }

  private static void requireCanonicalMarkers(
      List<MarkerDecision> decisions, OffsetRange decisionHorizon) {
    long previousOffset = -1;
    for (MarkerDecision decision : decisions) {
      Objects.requireNonNull(decision, "markerDecision");
      if (!decisionHorizon.contains(decision.markerOffset())
          || decision.markerOffset() <= previousOffset) {
        throw new IllegalArgumentException(
            "Kafka marker decisions are not canonical for the decision horizon");
      }
      previousOffset = decision.markerOffset();
    }
  }

  private static byte[] bytes(ByteBuffer buffer) {
    ByteBuffer exact = buffer.asReadOnlyBuffer();
    byte[] bytes = new byte[exact.remaining()];
    exact.get(bytes);
    return bytes;
  }

  private static final class ByteKey {
    private final byte[] bytes;

    private ByteKey(byte[] bytes) {
      this.bytes = Objects.requireNonNull(bytes, "bytes").clone();
    }

    private int length() {
      return bytes.length;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof ByteKey key && Arrays.equals(bytes, key.bytes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }
  }

  private static final class RecordFactDigest {
    private final MessageDigest digest;
    private boolean finished;

    private RecordFactDigest(byte[] domain) {
      try {
        digest = MessageDigest.getInstance("SHA-256");
      } catch (NoSuchAlgorithmException impossible) {
        throw new IllegalStateException("SHA-256 is unavailable", impossible);
      }
      digest.update(domain);
    }

    private void add(DecodedCompactionRecord record) {
      if (finished) {
        throw new IllegalStateException("Kafka compaction fact digest is finished");
      }
      putLong(record.absoluteOffset());
      digest.update((byte) record.keyKind().ordinal());
      digest.update((byte) record.controlKind().ordinal());
      putInt(record.coordinatorEpoch());
      putBytes(bytes(record.taggedCompactionKey()));
      digest.update((byte) (record.tombstone() ? 1 : 0));
      putOptionalLong(record.eventTimeMillis());
      putOptionalLong(record.deleteHorizonMillis());
      putLong(record.sourceBatchBaseOffset());
      putInt(record.sourceRecordIndex());
      putBytes(HexFormat.of().parseHex(record.sourceBatchSha256().value()));
      digest.update((byte) (record.transactional() ? 1 : 0));
      putLong(record.producerId());
      putInt(record.producerEpoch());
      putInt(record.sequence());
    }

    private Checksum finish() {
      if (finished) {
        throw new IllegalStateException("Kafka compaction fact digest is already finished");
      }
      finished = true;
      return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(digest.digest()));
    }

    private void putOptionalLong(OptionalLong value) {
      digest.update((byte) (value.isPresent() ? 1 : 0));
      if (value.isPresent()) {
        putLong(value.getAsLong());
      }
    }

    private void putBytes(byte[] value) {
      putInt(value.length);
      digest.update(value);
    }

    private void putInt(int value) {
      digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
    }

    private void putLong(long value) {
      digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
    }
  }
}
