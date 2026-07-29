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
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.materialization.CompactionRewriteContext;
import com.nereusstream.materialization.DecodedCompactionRecord;
import com.nereusstream.materialization.DecodedCompactionRecord.ControlKind;
import com.nereusstream.materialization.DecodedCompactionRecord.KeyKind;
import com.nereusstream.materialization.RangedTopicCompactionCodec;
import com.nereusstream.materialization.RewrittenCompactionRecord;
import com.nereusstream.materialization.RewrittenCompactionRecord.Disposition;
import com.nereusstream.objectstore.compacted.KafkaCompactionKeyEncodingV2;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.zip.CRC32C;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

/** Strict Kafka magic-v2 ranged decoder and one-record survivor rewriter. */
public final class KafkaTopicCompactionCodecV1 implements RangedTopicCompactionCodec {
  public static final String CODEC_ID = "kafka-topic-compaction-codec-v1";
  public static final long CODEC_VERSION = 1;
  public static final Checksum MESSAGE_FORMAT_SHA256 =
      new Checksum(
          ChecksumType.SHA256, "2940e62ac155a477052b955c1b30a2e7e77862bb7383d240b792ca064f472104");

  @Override
  public String codecId() {
    return CODEC_ID;
  }

  @Override
  public long codecVersion() {
    return CODEC_VERSION;
  }

  @Override
  public Checksum messageFormatSha256() {
    return MESSAGE_FORMAT_SHA256;
  }

  @Override
  public void decode(ReadBatch rangedBatch, DecodedRecordConsumer consumer) {
    Objects.requireNonNull(rangedBatch, "rangedBatch");
    Objects.requireNonNull(consumer, "consumer");
    if (rangedBatch.payloadFormat() != PayloadFormat.KAFKA_RECORD_BATCH) {
      throw new IllegalArgumentException("Kafka compaction requires KAFKA_RECORD_BATCH payloads");
    }
    byte[] exactBatch = rangedBatch.payload();
    BatchAndRecords decoded = decodeExactBatch(exactBatch);
    MutableRecordBatch batch = decoded.batch();
    if (batch.baseOffset() != rangedBatch.range().startOffset()
        || batch.nextOffset() != rangedBatch.range().endOffset()
        || decoded.records().size() != rangedBatch.range().recordCount()) {
      throw new IllegalArgumentException(
          "Kafka compaction batch bytes do not match the exact source range");
    }
    Checksum sourceSha = sha256(exactBatch);
    for (int index = 0; index < decoded.records().size(); index++) {
      consumer.accept(toDecoded(batch, decoded.records().get(index), index, sourceSha, exactBatch));
    }
  }

  @Override
  public RewrittenCompactionRecord rewrite(
      DecodedCompactionRecord survivor, CompactionRewriteContext context) {
    Objects.requireNonNull(survivor, "survivor");
    Objects.requireNonNull(context, "context");
    if (context.targetMagic() != RecordBatch.MAGIC_VALUE_V2
        || !MESSAGE_FORMAT_SHA256.equals(context.messageFormatSha256())) {
      throw new IllegalArgumentException(
          "Kafka compaction rewrite context does not match codec V1");
    }

    byte[] exactSource = bytes(survivor.rewriteToken());
    if (!survivor.sourceBatchSha256().equals(sha256(exactSource))) {
      throw new IllegalArgumentException("Kafka compaction rewrite token SHA-256 changed");
    }
    BatchAndRecords decoded = decodeExactBatch(exactSource);
    MutableRecordBatch batch = decoded.batch();
    if (batch.baseOffset() != survivor.sourceBatchBaseOffset()
        || survivor.sourceRecordIndex() >= decoded.records().size()) {
      throw new IllegalArgumentException(
          "Kafka compaction rewrite token does not contain the selected source record");
    }
    Record record = decoded.records().get(survivor.sourceRecordIndex());
    DecodedCompactionRecord redecoded =
        toDecoded(
            batch, record, survivor.sourceRecordIndex(), survivor.sourceBatchSha256(), exactSource);
    if (!sameFacts(survivor, redecoded)) {
      throw new IllegalArgumentException(
          "Kafka compaction rewrite token conflicts with selected survivor facts");
    }
    requireDeleteHorizon(survivor, context);

    byte[] rewritten =
        rewriteOne(
            batch,
            record,
            survivor.absoluteOffset(),
            context.deleteHorizonMillis(),
            context.allowUncompressedFallback());
    requireRoundTrip(batch, record, survivor, context.deleteHorizonMillis(), rewritten);
    return new RewrittenCompactionRecord(
        survivor.absoluteOffset(),
        disposition(survivor),
        survivor.taggedCompactionKey(),
        ByteBuffer.wrap(rewritten),
        crc32c(rewritten),
        survivor.sourceBatchBaseOffset(),
        survivor.sourceRecordIndex(),
        survivor.sourceBatchSha256(),
        survivor.eventTimeMillis());
  }

  private static DecodedCompactionRecord toDecoded(
      RecordBatch batch,
      Record record,
      int sourceRecordIndex,
      Checksum sourceSha,
      byte[] exactBatch) {
    record.ensureValid();
    long absoluteOffset = record.offset();
    if (absoluteOffset != Math.addExact(batch.baseOffset(), sourceRecordIndex)) {
      throw new IllegalArgumentException("Kafka compaction requires dense record offsets");
    }
    boolean control = batch.isControlBatch();
    KeyKind keyKind;
    ControlKind controlKind;
    int coordinatorEpoch;
    ByteBuffer taggedKey;
    boolean tombstone;
    if (control) {
      EndTransactionMarker marker = EndTransactionMarker.deserialize(record);
      keyKind = KeyKind.CONTROL;
      controlKind =
          switch (marker.controlType()) {
            case COMMIT -> ControlKind.COMMIT;
            case ABORT -> ControlKind.ABORT;
            default ->
                throw new IllegalArgumentException(
                    "Kafka compaction supports only commit/abort control markers");
          };
      coordinatorEpoch = marker.coordinatorEpoch();
      taggedKey = KafkaCompactionKeyEncodingV2.control(absoluteOffset);
      tombstone = false;
    } else if (record.hasKey()) {
      keyKind = KeyKind.KEYED;
      controlKind = ControlKind.NONE;
      coordinatorEpoch = -1;
      taggedKey = KafkaCompactionKeyEncodingV2.keyed(record.key());
      tombstone = !record.hasValue();
    } else {
      keyKind = KeyKind.UNKEYED;
      controlKind = ControlKind.NONE;
      coordinatorEpoch = -1;
      taggedKey = KafkaCompactionKeyEncodingV2.nullKey(absoluteOffset);
      tombstone = false;
    }
    OptionalLong eventTime =
        record.timestamp() < 0 ? OptionalLong.empty() : OptionalLong.of(record.timestamp());
    return new DecodedCompactionRecord(
        absoluteOffset,
        keyKind,
        controlKind,
        coordinatorEpoch,
        taggedKey,
        tombstone,
        eventTime,
        batch.deleteHorizonMs(),
        batch.baseOffset(),
        sourceRecordIndex,
        sourceSha,
        batch.isTransactional(),
        batch.producerId(),
        batch.producerEpoch(),
        producerSequence(batch, record),
        ByteBuffer.wrap(exactBatch));
  }

  private static byte[] rewriteOne(
      RecordBatch batch,
      Record record,
      long offset,
      OptionalLong deleteHorizonMillis,
      boolean allowUncompressedFallback) {
    try {
      return rewriteOne(batch, record, offset, deleteHorizonMillis, compression(batch));
    } catch (RuntimeException preserveFailure) {
      if (!allowUncompressedFallback || batch.compressionType().id == 0) {
        throw preserveFailure;
      }
      try {
        return rewriteOne(batch, record, offset, deleteHorizonMillis, Compression.NONE);
      } catch (RuntimeException fallbackFailure) {
        preserveFailure.addSuppressed(fallbackFailure);
        throw preserveFailure;
      }
    }
  }

  private static byte[] rewriteOne(
      RecordBatch batch,
      Record record,
      long offset,
      OptionalLong deleteHorizonMillis,
      Compression compression) {
    int initialCapacity = Math.max(1_024, Math.addExact(record.sizeInBytes(), 256));
    long logAppendTime =
        batch.timestampType() == TimestampType.LOG_APPEND_TIME
            ? batch.maxTimestamp()
            : RecordBatch.NO_TIMESTAMP;
    try (MemoryRecordsBuilder builder =
        new MemoryRecordsBuilder(
            new ByteBufferOutputStream(ByteBuffer.allocate(initialCapacity)),
            RecordBatch.MAGIC_VALUE_V2,
            compression,
            batch.timestampType(),
            offset,
            logAppendTime,
            batch.producerId(),
            batch.producerEpoch(),
            producerSequence(batch, record),
            batch.isTransactional(),
            batch.isControlBatch(),
            batch.partitionLeaderEpoch(),
            Integer.MAX_VALUE,
            deleteHorizonMillis.orElse(RecordBatch.NO_TIMESTAMP))) {
      if (batch.isControlBatch()) {
        builder.appendEndTxnMarker(record.timestamp(), EndTransactionMarker.deserialize(record));
      } else {
        builder.appendWithOffset(
            offset,
            record.timestamp(),
            nullableBytes(record.hasKey() ? record.key() : null),
            nullableBytes(record.hasValue() ? record.value() : null),
            copyHeaders(record.headers()));
      }
      builder.overrideLastOffset(offset);
      return bytes(builder.build().buffer());
    }
  }

  private static void requireRoundTrip(
      RecordBatch sourceBatch,
      Record source,
      DecodedCompactionRecord survivor,
      OptionalLong expectedDeleteHorizon,
      byte[] rewritten) {
    BatchAndRecords decoded = decodeExactBatch(rewritten);
    RecordBatch batch = decoded.batch();
    if (decoded.records().size() != 1
        || batch.baseOffset() != survivor.absoluteOffset()
        || batch.lastOffset() != survivor.absoluteOffset()
        || batch.timestampType() != sourceBatch.timestampType()
        || batch.partitionLeaderEpoch() != sourceBatch.partitionLeaderEpoch()
        || batch.isTransactional() != sourceBatch.isTransactional()
        || batch.isControlBatch() != sourceBatch.isControlBatch()
        || batch.producerId() != sourceBatch.producerId()
        || batch.producerEpoch() != sourceBatch.producerEpoch()
        || !batch.deleteHorizonMs().equals(expectedDeleteHorizon)
        || producerSequence(batch, decoded.records().get(0))
            != producerSequence(sourceBatch, source)
        || !sameRecord(source, decoded.records().get(0))) {
      throw new IllegalStateException(
          "Kafka compaction one-record rewrite failed its decode round trip");
    }
  }

  private static BatchAndRecords decodeExactBatch(byte[] exactBatch) {
    if (exactBatch.length == 0) {
      throw new IllegalArgumentException("Kafka compaction source batch cannot be empty");
    }
    MemoryRecords records = MemoryRecords.readableRecords(ByteBuffer.wrap(exactBatch));
    Iterator<MutableRecordBatch> batches = records.batches().iterator();
    if (!batches.hasNext()) {
      throw new IllegalArgumentException("Kafka compaction source contains no batch");
    }
    MutableRecordBatch batch = batches.next();
    if (batches.hasNext()
        || records.validBytes() != exactBatch.length
        || batch.magic() != RecordBatch.MAGIC_VALUE_V2
        || !batch.isValid()) {
      throw new IllegalArgumentException(
          "Kafka compaction source must contain one exact valid magic-v2 batch");
    }
    batch.ensureValid();
    java.util.ArrayList<Record> decoded = new java.util.ArrayList<>();
    for (Record record : batch) {
      record.ensureValid();
      decoded.add(record);
    }
    Integer declaredCount = batch.countOrNull();
    if (declaredCount == null || declaredCount != decoded.size() || decoded.isEmpty()) {
      throw new IllegalArgumentException("Kafka compaction batch record count is invalid");
    }
    return new BatchAndRecords(batch, decoded);
  }

  private static Compression compression(RecordBatch batch) {
    return Compression.of(batch.compressionType()).build();
  }

  private static int producerSequence(RecordBatch batch, Record record) {
    return batch.hasProducerId() ? record.sequence() : RecordBatch.NO_SEQUENCE;
  }

  private static Disposition disposition(DecodedCompactionRecord record) {
    return switch (record.keyKind()) {
      case CONTROL -> Disposition.RETAIN_CONTROL;
      case UNKEYED -> Disposition.RETAIN_UNKEYED;
      case KEYED -> record.tombstone() ? Disposition.RETAIN_TOMBSTONE : Disposition.RETAIN_VALUE;
    };
  }

  private static boolean sameFacts(
      DecodedCompactionRecord expected, DecodedCompactionRecord actual) {
    return expected.absoluteOffset() == actual.absoluteOffset()
        && expected.keyKind() == actual.keyKind()
        && expected.controlKind() == actual.controlKind()
        && expected.coordinatorEpoch() == actual.coordinatorEpoch()
        && Arrays.equals(bytes(expected.taggedCompactionKey()), bytes(actual.taggedCompactionKey()))
        && expected.tombstone() == actual.tombstone()
        && expected.eventTimeMillis().equals(actual.eventTimeMillis())
        && expected.deleteHorizonMillis().equals(actual.deleteHorizonMillis())
        && expected.sourceBatchBaseOffset() == actual.sourceBatchBaseOffset()
        && expected.sourceRecordIndex() == actual.sourceRecordIndex()
        && expected.sourceBatchSha256().equals(actual.sourceBatchSha256())
        && expected.transactional() == actual.transactional()
        && expected.producerId() == actual.producerId()
        && expected.producerEpoch() == actual.producerEpoch()
        && expected.sequence() == actual.sequence();
  }

  private static void requireDeleteHorizon(
      DecodedCompactionRecord survivor, CompactionRewriteContext context) {
    OptionalLong source = survivor.deleteHorizonMillis();
    OptionalLong target = context.deleteHorizonMillis();
    if (source.isPresent() && !source.equals(target)) {
      throw new IllegalArgumentException(
          "Kafka compaction rewrite must preserve an existing delete horizon");
    }
    if (source.isEmpty()
        && survivor.keyKind() == KeyKind.KEYED
        && !survivor.tombstone()
        && target.isPresent()) {
      throw new IllegalArgumentException("Kafka value survivor cannot introduce a delete horizon");
    }
    if (source.isEmpty() && survivor.keyKind() == KeyKind.UNKEYED && target.isPresent()) {
      throw new IllegalArgumentException(
          "Kafka unkeyed survivor cannot introduce a delete horizon");
    }
  }

  private static boolean sameRecord(Record expected, Record actual) {
    return expected.offset() == actual.offset()
        && expected.sequence() == actual.sequence()
        && expected.timestamp() == actual.timestamp()
        && Arrays.equals(
            nullableBytes(expected.hasKey() ? expected.key() : null),
            nullableBytes(actual.hasKey() ? actual.key() : null))
        && Arrays.equals(
            nullableBytes(expected.hasValue() ? expected.value() : null),
            nullableBytes(actual.hasValue() ? actual.value() : null))
        && Arrays.equals(copyHeaders(expected.headers()), copyHeaders(actual.headers()));
  }

  private static Header[] copyHeaders(Header[] headers) {
    Header[] copy = new Header[headers.length];
    for (int index = 0; index < headers.length; index++) {
      Header header = Objects.requireNonNull(headers[index], "Kafka record header");
      copy[index] =
          new RecordHeader(
              Objects.requireNonNull(header.key(), "Kafka record header key"),
              header.value() == null ? null : header.value().clone());
    }
    return copy;
  }

  private static byte[] nullableBytes(ByteBuffer buffer) {
    return buffer == null ? null : bytes(buffer);
  }

  private static byte[] bytes(ByteBuffer buffer) {
    ByteBuffer exact = buffer.asReadOnlyBuffer();
    byte[] bytes = new byte[exact.remaining()];
    exact.get(bytes);
    return bytes;
  }

  private static int crc32c(byte[] bytes) {
    CRC32C crc32c = new CRC32C();
    crc32c.update(bytes, 0, bytes.length);
    return (int) crc32c.getValue();
  }

  private static Checksum sha256(byte[] bytes) {
    try {
      return new Checksum(
          ChecksumType.SHA256,
          HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }

  private record BatchAndRecords(MutableRecordBatch batch, List<Record> records) {
    private BatchAndRecords {
      Objects.requireNonNull(batch, "batch");
      records = List.copyOf(Objects.requireNonNull(records, "records"));
    }
  }
}
