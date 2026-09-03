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

package com.nereusstream.kafka.bookkeeper.compaction;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.ControlKind;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.HeaderValue;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.ParsedBatch;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.RecordValue;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;

/** Strict one-batch Kafka magic-v2 parser and deterministic sparse-batch rewriter. */
public final class KafkaRecordBatchCodecV1 {
    private KafkaRecordBatchCodecV1() {}

    public static ParsedBatch parse(CanonicalBytes body) {
        Objects.requireNonNull(body, "body");
        if (body.isEmpty() || body.length() > KafkaCompactionRecordsV1.MAX_BATCH_BYTES) {
            throw new IllegalArgumentException("Kafka RecordBatch body length is outside the M5-B cap");
        }
        MemoryRecords records = MemoryRecords.readableRecords(ByteBuffer.wrap(body.toByteArray()));
        if (records.validBytes() != body.length()) {
            throw new IllegalArgumentException("Kafka RecordBatch has truncated or trailing bytes");
        }
        List<MutableRecordBatch> batches = new ArrayList<>(2);
        records.batches().forEach(batches::add);
        if (batches.size() != 1) {
            throw new IllegalArgumentException("M5-B input must contain exactly one complete RecordBatch");
        }
        MutableRecordBatch batch = batches.get(0);
        batch.ensureValid();
        if (batch.magic() != RecordBatch.MAGIC_VALUE_V2
                || batch.baseOffset() < 0
                || batch.lastOffset() < batch.baseOffset()
                || batch.partitionLeaderEpoch() < 0) {
            throw new IllegalArgumentException("M5-B accepts only assigned magic-v2 RecordBatch input");
        }
        List<RecordValue> values = new ArrayList<>();
        try (CloseableIterator<Record> iterator = batch.streamingIterator(BufferSupplier.NO_CACHING)) {
            while (iterator.hasNext()) {
                Record record = iterator.next();
                record.ensureValid();
                values.add(new RecordValue(
                        record.offset(),
                        record.sequence(),
                        record.timestamp(),
                        optionalBytes(record.hasKey(), record.key()),
                        optionalBytes(record.hasValue(), record.value()),
                        headers(record.headers())));
            }
        }
        if (batch.countOrNull() != null && batch.countOrNull() != values.size()) {
            throw new IllegalArgumentException("Kafka RecordBatch count differs from parsed records");
        }
        ControlKind controlKind = controlKind(batch, values);
        return new ParsedBatch(
                body,
                Sha256Digest.hash(body),
                batch.baseOffset(),
                batch.lastOffset(),
                batch.partitionLeaderEpoch(),
                batch.magic(),
                batch.compressionType(),
                batch.timestampType(),
                batch.maxTimestamp(),
                batch.producerId(),
                batch.producerEpoch(),
                batch.baseSequence(),
                batch.isTransactional(),
                controlKind,
                values);
    }

    public static boolean canRewriteSubset(ParsedBatch batch, List<RecordValue> retained) {
        Objects.requireNonNull(batch, "batch");
        retained = List.copyOf(Objects.requireNonNull(retained, "retained"));
        if (batch.controlKind() != ControlKind.NONE) {
            return retained.equals(batch.records());
        }
        if (retained.isEmpty()
                || retained.equals(batch.records())
                || batch.producerId() == RecordBatch.NO_PRODUCER_ID) {
            return true;
        }
        for (RecordValue record : retained) {
            long delta = record.offset() - batch.baseOffset();
            if (delta < 0 || delta > Integer.MAX_VALUE) {
                return false;
            }
            int expected = DefaultRecordBatch.incrementSequence(batch.baseSequence(), (int) delta);
            if (record.sequence() != expected) {
                return false;
            }
        }
        return true;
    }

    /** Empty means the complete batch became an explicit manifest/index gap. */
    public static Optional<CanonicalBytes> rewrite(
            ParsedBatch batch, List<RecordValue> retained, org.apache.kafka.common.record.CompressionType compression) {
        Objects.requireNonNull(batch, "batch");
        retained = List.copyOf(Objects.requireNonNull(retained, "retained"));
        Objects.requireNonNull(compression, "compression");
        if (retained.isEmpty()) {
            return Optional.empty();
        }
        if (batch.controlKind() != ControlKind.NONE && retained.equals(batch.records())) {
            return Optional.of(batch.canonicalBody());
        }
        if (retained.equals(batch.records()) && compression == batch.compressionType()) {
            return Optional.of(batch.canonicalBody());
        }
        if (!canRewriteSubset(batch, retained)) {
            throw new IllegalArgumentException("Kafka producer sequence cannot be expressed by the sparse rewrite");
        }
        int estimated = Math.addExact(
                Math.max(batch.canonicalBody().length(), 1024),
                Math.min(batch.canonicalBody().length(), 8 * 1024 * 1024));
        if (estimated > KafkaCompactionRecordsV1.MAX_BATCH_BYTES) {
            throw new IllegalArgumentException("Kafka sparse rewrite buffer exceeds the M5-B batch cap");
        }
        ByteBuffer target = ByteBuffer.allocate(estimated);
        Compression outputCompression = Compression.of(compression).build();
        MemoryRecordsBuilder builder = MemoryRecords.builder(
                target,
                batch.magic(),
                outputCompression,
                batch.timestampType(),
                batch.baseOffset(),
                batch.maxTimestamp(),
                batch.producerId(),
                batch.producerEpoch(),
                batch.baseSequence(),
                batch.transactional(),
                false,
                batch.partitionLeaderEpoch());
        try (builder) {
            for (RecordValue record : retained) {
                builder.appendWithOffset(
                        record.offset(),
                        record.timestamp(),
                        record.key().map(CanonicalBytes::toByteArray).orElse(null),
                        record.value().map(CanonicalBytes::toByteArray).orElse(null),
                        headers(record.headers()));
            }
            MemoryRecords built = builder.build();
            ByteBuffer encoded = built.buffer().duplicate();
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            CanonicalBytes result = CanonicalBytes.copyOf(bytes);
            ParsedBatch reparsed = parse(result);
            if (!reparsed.records().equals(retained)
                    || reparsed.producerId() != batch.producerId()
                    || reparsed.producerEpoch() != batch.producerEpoch()
                    || reparsed.baseSequence() != batch.baseSequence()
                    || reparsed.transactional() != batch.transactional()
                    || reparsed.partitionLeaderEpoch() != batch.partitionLeaderEpoch()) {
                throw new IllegalStateException("Kafka sparse rewrite did not preserve exact record semantics");
            }
            return Optional.of(result);
        }
    }

    private static ControlKind controlKind(MutableRecordBatch batch, List<RecordValue> values) {
        if (!batch.isControlBatch()) {
            return ControlKind.NONE;
        }
        if (values.size() != 1 || values.get(0).key().isEmpty()) {
            throw new IllegalArgumentException("Kafka control batch does not contain exactly one marker");
        }
        ControlRecordType type = ControlRecordType.parse(
                ByteBuffer.wrap(values.get(0).key().orElseThrow().toByteArray()));
        return switch (type) {
            case COMMIT -> ControlKind.COMMIT;
            case ABORT -> ControlKind.ABORT;
            default -> throw new IllegalArgumentException("Kafka control marker type is outside M5-B");
        };
    }

    private static Optional<CanonicalBytes> optionalBytes(boolean present, ByteBuffer value) {
        if (!present) {
            return Optional.empty();
        }
        ByteBuffer copy = Objects.requireNonNull(value, "Kafka record field").duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return Optional.of(CanonicalBytes.copyOf(bytes));
    }

    private static List<HeaderValue> headers(Header[] values) {
        if (values == null || values.length == 0) {
            return List.of();
        }
        return Arrays.stream(values)
                .map(value -> new HeaderValue(
                        value.key(), Optional.ofNullable(value.value()).map(CanonicalBytes::copyOf)))
                .toList();
    }

    private static Header[] headers(List<HeaderValue> values) {
        return values.stream()
                .map(value -> new RecordHeader(
                        value.key(),
                        value.value().map(CanonicalBytes::toByteArray).orElse(null)))
                .toArray(Header[]::new);
    }
}
