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
import com.nereusstream.api.StreamId;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.AbortedTransactionRange;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.MarkerDecision;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.OpenTransactionRange;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.Snapshot;
import com.nereusstream.kafka.compaction.KafkaCompactionPlan.Compatibility;
import com.nereusstream.kafka.compaction.KafkaCompactionPlanner.Candidate;
import com.nereusstream.kafka.compaction.KafkaCompactionPlanner.MandatoryCoverage;
import com.nereusstream.kafka.compaction.KafkaCompactionPlanner.Policy;
import com.nereusstream.materialization.ExactSourceSet;
import com.nereusstream.materialization.ExactSourceSetCodecV1;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;

/** Strict restart-safe V1 codec for {@link KafkaCompactionPlan}. */
public final class KafkaCompactionPlanCodecV1 {
  public static final int MAX_ENCODED_BYTES = 60 << 10;
  public static final int MAX_TRANSACTION_FACTS =
      KafkaCompactionPassOneCollector.MAX_TRANSACTION_FACTS;

  private static final int MAGIC = 0x4b435031; // KCP1
  private static final int VERSION = 1;
  private static final int MAX_STRING_BYTES = 65_535;
  private static final String IDENTITY_DOMAIN = "NEREUS_KAFKA_COMPACTION_PLAN_V1";

  private final ExactSourceSetCodecV1 sourceSetCodec;

  public KafkaCompactionPlanCodecV1() {
    this(new ExactSourceSetCodecV1());
  }

  KafkaCompactionPlanCodecV1(ExactSourceSetCodecV1 sourceSetCodec) {
    this.sourceSetCodec = Objects.requireNonNull(sourceSetCodec, "sourceSetCodec");
  }

  public byte[] encode(KafkaCompactionPlan plan) {
    KafkaCompactionPlan exact = Objects.requireNonNull(plan, "plan");
    try {
      Writer writer = new Writer();
      writer.intValue(MAGIC);
      writer.shortValue(VERSION);
      writer.text(exact.planId());
      writer.raw(canonicalBody(exact));
      byte[] encoded = writer.bytes();
      if (encoded.length > MAX_ENCODED_BYTES) {
        throw new IllegalArgumentException("Kafka compaction plan exceeds its byte limit");
      }
      return encoded;
    } catch (IOException failure) {
      throw new IllegalStateException("in-memory Kafka compaction plan encoding failed", failure);
    }
  }

  public KafkaCompactionPlan decode(byte[] bytes) {
    byte[] exact = Objects.requireNonNull(bytes, "bytes").clone();
    if (exact.length == 0 || exact.length > MAX_ENCODED_BYTES) {
      throw malformed("payload has an invalid length", null);
    }
    try {
      Reader reader = new Reader(exact);
      if (reader.intValue("magic") != MAGIC || reader.unsignedShort("version") != VERSION) {
        throw malformed("unsupported header", null);
      }
      String planId = reader.text("planId");
      if (!reader.text("identityDomain").equals(IDENTITY_DOMAIN)) {
        throw malformed("identity domain changed", null);
      }
      StreamId streamId = new StreamId(reader.text("streamId"));
      String materializationTaskId = reader.text("materializationTaskId");
      long bindingMetadataVersion = reader.longValue("bindingMetadataVersion");
      long lastStableOffset = reader.longValue("lastStableOffset");
      long highWatermark = reader.longValue("highWatermark");
      Candidate candidate = readCandidate(reader);
      ExactSourceSet decisionSources =
          sourceSetCodec.decode(
              reader.byteArray("decisionSources", ExactSourceSetCodecV1.MAX_ENCODED_BYTES));
      int outputSourceCount = reader.intValue("outputSourceCount");
      Checksum outputSourceSetSha256 = reader.checksum("outputSourceSetSha256");
      Checksum materializationPolicySha256 = reader.checksum("materializationPolicySha256");
      Snapshot snapshot = readSnapshot(reader);
      Compatibility compatibility = readCompatibility(reader);
      reader.requireConsumed();
      return new KafkaCompactionPlan(
          planId,
          streamId,
          materializationTaskId,
          bindingMetadataVersion,
          lastStableOffset,
          highWatermark,
          candidate,
          decisionSources,
          outputSourceCount,
          outputSourceSetSha256,
          materializationPolicySha256,
          snapshot,
          compatibility);
    } catch (IllegalArgumentException failure) {
      if (failure.getMessage() != null
          && failure.getMessage().startsWith("malformed Kafka compaction plan:")) {
        throw failure;
      }
      throw malformed("invalid canonical fields: " + failure.getMessage(), failure);
    }
  }

  static String planIdFor(
      StreamId streamId,
      String materializationTaskId,
      long bindingMetadataVersion,
      long lastStableOffset,
      long highWatermark,
      Candidate candidate,
      ExactSourceSet decisionSources,
      int outputSourceCount,
      Checksum outputSourceSetSha256,
      Checksum materializationPolicySha256,
      Snapshot snapshot,
      Compatibility compatibility) {
    KafkaCompactionPlanCodecV1 codec = new KafkaCompactionPlanCodecV1();
    try {
      byte[] body =
          codec.canonicalBody(
              streamId,
              materializationTaskId,
              bindingMetadataVersion,
              lastStableOffset,
              highWatermark,
              candidate,
              decisionSources,
              outputSourceCount,
              outputSourceSetSha256,
              materializationPolicySha256,
              snapshot,
              compatibility);
      return "kcp1-" + DeterministicIds.stableHashBytes(body);
    } catch (IOException failure) {
      throw new IllegalStateException("in-memory Kafka compaction plan identity failed", failure);
    }
  }

  private byte[] canonicalBody(KafkaCompactionPlan plan) throws IOException {
    return canonicalBody(
        plan.streamId(),
        plan.materializationTaskId(),
        plan.bindingMetadataVersion(),
        plan.lastStableOffset(),
        plan.highWatermark(),
        plan.candidate(),
        plan.decisionSources(),
        plan.outputSourceCount(),
        plan.outputSourceSetSha256(),
        plan.materializationPolicySha256(),
        plan.passOneSnapshot(),
        plan.compatibility());
  }

  private byte[] canonicalBody(
      StreamId streamId,
      String materializationTaskId,
      long bindingMetadataVersion,
      long lastStableOffset,
      long highWatermark,
      Candidate candidate,
      ExactSourceSet decisionSources,
      int outputSourceCount,
      Checksum outputSourceSetSha256,
      Checksum materializationPolicySha256,
      Snapshot snapshot,
      Compatibility compatibility)
      throws IOException {
    Writer writer = new Writer();
    writer.text(IDENTITY_DOMAIN);
    writer.text(Objects.requireNonNull(streamId, "streamId").value());
    writer.text(Objects.requireNonNull(materializationTaskId, "materializationTaskId"));
    writer.longValue(bindingMetadataVersion);
    writer.longValue(lastStableOffset);
    writer.longValue(highWatermark);
    writeCandidate(writer, Objects.requireNonNull(candidate, "candidate"));
    writer.byteArray(
        sourceSetCodec.encode(Objects.requireNonNull(decisionSources, "decisionSources")),
        ExactSourceSetCodecV1.MAX_ENCODED_BYTES);
    writer.intValue(outputSourceCount);
    writer.checksum(outputSourceSetSha256);
    writer.checksum(materializationPolicySha256);
    writeSnapshot(writer, Objects.requireNonNull(snapshot, "snapshot"));
    writeCompatibility(writer, Objects.requireNonNull(compatibility, "compatibility"));
    return writer.bytes();
  }

  private static void writeCandidate(Writer writer, Candidate candidate) throws IOException {
    writer.range(candidate.outputCoverage());
    writer.range(candidate.decisionHorizon());
    writer.intValue(candidate.selectedSegmentCount());
    writePolicy(writer, candidate.policy());
    writer.optional(candidate.previousMandatoryCoverage().isPresent());
    if (candidate.previousMandatoryCoverage().isPresent()) {
      MandatoryCoverage coverage = candidate.previousMandatoryCoverage().orElseThrow();
      writer.longValue(coverage.startOffset());
      writer.longValue(coverage.endOffset());
      writer.longValue(coverage.activationEpoch());
      writer.checksum(coverage.generationSetSha256());
      writer.checksum(coverage.policySha256());
    }
    writer.longValue(candidate.evaluatedAtMillis());
  }

  private static Candidate readCandidate(Reader reader) {
    OffsetRange outputCoverage = reader.range("outputCoverage");
    OffsetRange decisionHorizon = reader.range("decisionHorizon");
    int selectedSegmentCount = reader.intValue("selectedSegmentCount");
    Policy policy = readPolicy(reader);
    Optional<MandatoryCoverage> previous =
        reader.optional("previousMandatoryCoveragePresent")
            ? Optional.of(
                new MandatoryCoverage(
                    reader.longValue("mandatoryStart"),
                    reader.longValue("mandatoryEnd"),
                    reader.longValue("mandatoryActivationEpoch"),
                    reader.checksum("mandatoryGenerationSetSha256"),
                    reader.checksum("mandatoryPolicySha256")))
            : Optional.empty();
    return new Candidate(
        outputCoverage,
        decisionHorizon,
        selectedSegmentCount,
        policy,
        previous,
        reader.longValue("evaluatedAtMillis"));
  }

  private static void writePolicy(Writer writer, Policy policy) throws IOException {
    writer.longValue(policy.metadataOffset());
    writer.checksum(policy.configDigest());
    writer.longValue(policy.minCompactionLagMs());
    writer.longValue(policy.maxCompactionLagMs());
    writer.longValue(policy.deleteRetentionMs());
    writer.intValue(policy.cleanupPolicyFlags());
  }

  private static Policy readPolicy(Reader reader) {
    return new Policy(
        reader.longValue("policyMetadataOffset"),
        reader.checksum("policyConfigDigest"),
        reader.longValue("minCompactionLagMs"),
        reader.longValue("maxCompactionLagMs"),
        reader.longValue("deleteRetentionMs"),
        reader.intValue("cleanupPolicyFlags"));
  }

  private static void writeSnapshot(Writer writer, Snapshot snapshot) throws IOException {
    writer.range(snapshot.outputCoverage());
    writer.range(snapshot.decisionHorizon());
    writer.longValue(snapshot.transactionStateEndOffset());
    writer.longValue(snapshot.nowMillis());
    writer.longValue(snapshot.deleteRetentionMs());
    writer.longValue(snapshot.maxDecodedRecords());
    writer.intValue(snapshot.maxKeyBytes());
    writer.longValue(snapshot.maxInMemoryKeyBytes());
    writer.intValue(snapshot.abortedTransactions().size());
    for (AbortedTransactionRange aborted : snapshot.abortedTransactions()) {
      writer.longValue(aborted.producerId());
      writer.longValue(aborted.firstOffset());
      writer.longValue(aborted.markerOffset());
    }
    writer.intValue(snapshot.openTransactions().size());
    for (OpenTransactionRange open : snapshot.openTransactions()) {
      writer.longValue(open.producerId());
      writer.longValue(open.firstOffset());
    }
    writer.intValue(snapshot.markerDecisions().size());
    for (MarkerDecision marker : snapshot.markerDecisions()) {
      writer.longValue(marker.markerOffset());
      writer.intValue(marker.status().ordinal());
    }
  }

  private static Snapshot readSnapshot(Reader reader) {
    OffsetRange outputCoverage = reader.range("snapshotOutputCoverage");
    OffsetRange decisionHorizon = reader.range("snapshotDecisionHorizon");
    long transactionStateEnd = reader.longValue("transactionStateEndOffset");
    long nowMillis = reader.longValue("snapshotNowMillis");
    long deleteRetentionMs = reader.longValue("snapshotDeleteRetentionMs");
    long maxDecodedRecords = reader.longValue("maxDecodedRecords");
    int maxKeyBytes = reader.intValue("maxKeyBytes");
    long maxInMemoryKeyBytes = reader.longValue("maxInMemoryKeyBytes");
    int abortedCount =
        reader.count("abortedTransactionCount", MAX_TRANSACTION_FACTS, Long.BYTES * 3);
    ArrayList<AbortedTransactionRange> aborted = new ArrayList<>(abortedCount);
    for (int index = 0; index < abortedCount; index++) {
      aborted.add(
          new AbortedTransactionRange(
              reader.longValue("abortedProducerId"),
              reader.longValue("abortedFirstOffset"),
              reader.longValue("abortedMarkerOffset")));
    }
    int openCount = reader.count("openTransactionCount", MAX_TRANSACTION_FACTS, Long.BYTES * 2);
    ArrayList<OpenTransactionRange> open = new ArrayList<>(openCount);
    for (int index = 0; index < openCount; index++) {
      open.add(
          new OpenTransactionRange(
              reader.longValue("openProducerId"), reader.longValue("openFirstOffset")));
    }
    int markerCount =
        reader.count("markerDecisionCount", MAX_TRANSACTION_FACTS, Long.BYTES + Integer.BYTES);
    ArrayList<MarkerDecision> markers = new ArrayList<>(markerCount);
    KafkaCompactionStrategyV1.MarkerStatus[] statuses =
        KafkaCompactionStrategyV1.MarkerStatus.values();
    for (int index = 0; index < markerCount; index++) {
      long markerOffset = reader.longValue("markerOffset");
      int statusId = reader.intValue("markerStatus");
      if (statusId < 0 || statusId >= statuses.length) {
        throw malformed("unknown marker status", null);
      }
      markers.add(new MarkerDecision(markerOffset, statuses[statusId]));
    }
    return new Snapshot(
        outputCoverage,
        decisionHorizon,
        transactionStateEnd,
        nowMillis,
        deleteRetentionMs,
        maxDecodedRecords,
        maxKeyBytes,
        maxInMemoryKeyBytes,
        aborted,
        open,
        markers);
  }

  private static void writeCompatibility(Writer writer, Compatibility compatibility)
      throws IOException {
    writer.text(compatibility.strategyId());
    writer.longValue(compatibility.strategyVersion());
    writer.text(compatibility.keyCodecId());
    writer.text(compatibility.rewriteCodecId());
    writer.checksum(compatibility.messageFormatSha256());
  }

  private static Compatibility readCompatibility(Reader reader) {
    return new Compatibility(
        reader.text("strategyId"),
        reader.longValue("strategyVersion"),
        reader.text("keyCodecId"),
        reader.text("rewriteCodecId"),
        reader.checksum("messageFormatSha256"));
  }

  private static IllegalArgumentException malformed(String message, Throwable cause) {
    return new IllegalArgumentException("malformed Kafka compaction plan: " + message, cause);
  }

  private static final class Writer {
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    private final DataOutputStream output = new DataOutputStream(bytes);

    void shortValue(int value) throws IOException {
      if (value < 0 || value > 0xffff) {
        throw new IllegalArgumentException("unsigned short value is out of range");
      }
      output.writeShort(value);
    }

    void intValue(int value) throws IOException {
      output.writeInt(value);
    }

    void longValue(long value) throws IOException {
      output.writeLong(value);
    }

    void range(OffsetRange range) throws IOException {
      output.writeLong(range.startOffset());
      output.writeLong(range.endOffset());
    }

    void optional(boolean present) throws IOException {
      output.writeByte(present ? 1 : 0);
    }

    void checksum(Checksum checksum) throws IOException {
      Objects.requireNonNull(checksum, "checksum");
      if (checksum.type() != ChecksumType.SHA256) {
        throw new IllegalArgumentException("Kafka compaction plan checksum must use SHA256");
      }
      text(checksum.value());
    }

    void text(String value) throws IOException {
      byte[] encoded = Objects.requireNonNull(value, "value").getBytes(StandardCharsets.UTF_8);
      if (encoded.length > MAX_STRING_BYTES) {
        throw new IllegalArgumentException("Kafka compaction plan string exceeds its byte limit");
      }
      output.writeInt(encoded.length);
      output.write(encoded);
    }

    void byteArray(byte[] value, int maximum) throws IOException {
      byte[] exact = Objects.requireNonNull(value, "value");
      if (exact.length > maximum) {
        throw new IllegalArgumentException("Kafka compaction plan byte array exceeds its limit");
      }
      output.writeInt(exact.length);
      output.write(exact);
    }

    void raw(byte[] value) throws IOException {
      output.write(value);
    }

    byte[] bytes() throws IOException {
      output.flush();
      return bytes.toByteArray();
    }
  }

  private static final class Reader {
    private final ByteBuffer input;

    Reader(byte[] bytes) {
      input = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
    }

    int unsignedShort(String field) {
      require(Short.BYTES, field);
      return Short.toUnsignedInt(input.getShort());
    }

    int intValue(String field) {
      require(Integer.BYTES, field);
      return input.getInt();
    }

    long longValue(String field) {
      require(Long.BYTES, field);
      return input.getLong();
    }

    OffsetRange range(String field) {
      return new OffsetRange(longValue(field + "Start"), longValue(field + "End"));
    }

    boolean optional(String field) {
      require(1, field);
      byte value = input.get();
      if (value != 0 && value != 1) {
        throw malformed(field + " is not a canonical boolean", null);
      }
      return value == 1;
    }

    Checksum checksum(String field) {
      return new Checksum(ChecksumType.SHA256, text(field));
    }

    String text(String field) {
      byte[] bytes = byteArray(field, MAX_STRING_BYTES);
      try {
        return StandardCharsets.UTF_8
            .newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT)
            .decode(ByteBuffer.wrap(bytes))
            .toString();
      } catch (CharacterCodingException failure) {
        throw malformed(field + " is not strict UTF-8", failure);
      }
    }

    byte[] byteArray(String field, int maximum) {
      int length = intValue(field + "Length");
      if (length < 0 || length > maximum) {
        throw malformed(field + " length exceeds its bound", null);
      }
      require(length, field);
      byte[] value = new byte[length];
      input.get(value);
      return value;
    }

    int count(String field, int maximum, int minimumBytesPerItem) {
      int count = intValue(field);
      if (count < 0
          || count > maximum
          || (minimumBytesPerItem > 0 && count > input.remaining() / minimumBytesPerItem)) {
        throw malformed(field + " is outside its bound", null);
      }
      return count;
    }

    void requireConsumed() {
      if (input.hasRemaining()) {
        throw malformed("payload contains trailing bytes", null);
      }
    }

    private void require(int bytes, String field) {
      if (bytes < 0 || input.remaining() < bytes) {
        throw malformed("truncated " + field, null);
      }
    }
  }
}
