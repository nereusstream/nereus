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
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/** One canonical M5-B rebuilt index with floor, coverage, then successor gap behavior. */
public record KafkaCompactionIndexV1(
        IndexKind kind,
        ProtocolCoverage coverage,
        Sha256Digest materializationTaskIdSha256,
        Sha256Digest outputIdentitySha256,
        List<Row> rows) {
    private static final int MAGIC = 0x4b354931; // K5I1
    private static final int VERSION = 1;
    public static final int FLAG_RETAINED = 1;
    public static final int FLAG_GAP = 1 << 1;
    public static final int FLAG_TRANSACTIONAL = 1 << 2;
    public static final int FLAG_CONTROL = 1 << 3;
    public static final int FLAG_ABORTED = 1 << 4;
    private static final int KNOWN_FLAGS = FLAG_RETAINED | FLAG_GAP | FLAG_TRANSACTIONAL | FLAG_CONTROL | FLAG_ABORTED;

    public KafkaCompactionIndexV1 {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(coverage, "coverage");
        KafkaCompactionRecordsV1.requireDigest(materializationTaskIdSha256, "materializationTaskIdSha256");
        KafkaCompactionRecordsV1.requireDigest(outputIdentitySha256, "outputIdentitySha256");
        rows = List.copyOf(Objects.requireNonNull(rows, "rows"));
        if (rows.size() > KafkaCompactionRecordsV1.MAX_RECORDS) {
            throw new IllegalArgumentException("M5-B rebuilt index row count exceeds its cap");
        }
        if (!rows.equals(rows.stream()
                .sorted(Comparator.comparingLong(value -> value.coverage().inclusiveStart()))
                .toList())) {
            throw new IllegalArgumentException("M5-B rebuilt index rows are not sorted");
        }
        for (int index = 0; index < rows.size(); index++) {
            Row row = rows.get(index);
            if (!coverage.contains(row.coverage())
                    || row.coverage().domain() != PositionDomain.KAFKA_OFFSET
                    || (index > 0
                            && rows.get(index - 1).coverage().exclusiveEnd()
                                    > row.coverage().inclusiveStart())) {
                throw new IllegalArgumentException("M5-B rebuilt index row overlaps or escapes coverage");
            }
        }
    }

    public record Row(
            ProtocolCoverage coverage,
            int outputBatchOrdinal,
            long byteOffset,
            int byteLength,
            long minimumTimestamp,
            long maximumTimestamp,
            long producerId,
            short producerEpoch,
            int sequence,
            int leaderEpoch,
            int flags,
            Sha256Digest identitySha256) {
        public Row {
            Objects.requireNonNull(coverage, "coverage");
            KafkaCompactionRecordsV1.requireDigest(identitySha256, "identitySha256");
            if (outputBatchOrdinal < -1
                    || byteOffset < 0
                    || byteLength < 0
                    || minimumTimestamp < -1
                    || maximumTimestamp < minimumTimestamp
                    || producerId < -1
                    || producerEpoch < -1
                    || sequence < -1
                    || leaderEpoch < -1
                    || flags <= 0
                    || (flags & ~KNOWN_FLAGS) != 0) {
                throw new IllegalArgumentException("M5-B rebuilt index row is outside its domain");
            }
            boolean gap = (flags & FLAG_GAP) != 0;
            if (gap != (byteLength == 0) || gap != (outputBatchOrdinal == -1)) {
                throw new IllegalArgumentException("M5-B rebuilt index gap row has physical bytes");
            }
        }

        public boolean covers(long offset) {
            return coverage.inclusiveStart() <= offset && offset < coverage.exclusiveEnd();
        }
    }

    public Optional<Row> lookup(long requestedOffset) {
        if (requestedOffset < coverage.inclusiveStart() || requestedOffset >= coverage.exclusiveEnd()) {
            return Optional.empty();
        }
        Row floor = null;
        for (Row row : rows) {
            if ((row.flags() & FLAG_GAP) != 0) {
                continue;
            }
            if (row.coverage().inclusiveStart() <= requestedOffset) {
                floor = row;
                continue;
            }
            return floor != null && floor.covers(requestedOffset) ? Optional.of(floor) : Optional.of(row);
        }
        return floor != null && floor.covers(requestedOffset) ? Optional.of(floor) : Optional.empty();
    }

    public OptionalLong listOffset(long targetTimestamp) {
        if (kind != IndexKind.TIMESTAMP || targetTimestamp < 0) {
            throw new IllegalArgumentException("ListOffsets requires the rebuilt timestamp index and nonnegative time");
        }
        return rows.stream()
                .filter(row -> (row.flags() & FLAG_GAP) == 0 && row.maximumTimestamp() >= targetTimestamp)
                .mapToLong(row -> row.coverage().inclusiveStart())
                .findFirst();
    }

    public CanonicalBytes encode() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(MAGIC);
                output.writeInt(VERSION);
                output.writeByte(kind.ordinal());
                writeCoverage(output, coverage);
                writeDigest(output, materializationTaskIdSha256);
                writeDigest(output, outputIdentitySha256);
                output.writeInt(rows.size());
                for (Row row : rows) {
                    writeCoverage(output, row.coverage());
                    output.writeInt(row.outputBatchOrdinal());
                    output.writeLong(row.byteOffset());
                    output.writeInt(row.byteLength());
                    output.writeLong(row.minimumTimestamp());
                    output.writeLong(row.maximumTimestamp());
                    output.writeLong(row.producerId());
                    output.writeShort(row.producerEpoch());
                    output.writeInt(row.sequence());
                    output.writeInt(row.leaderEpoch());
                    output.writeInt(row.flags());
                    writeDigest(output, row.identitySha256());
                }
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory M5-B index encoding failed", impossible);
        }
    }

    public static KafkaCompactionIndexV1 decode(CanonicalBytes encoded) {
        Objects.requireNonNull(encoded, "encoded");
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded.toByteArray()))) {
            if (input.readInt() != MAGIC || input.readInt() != VERSION) {
                throw new IllegalArgumentException("M5-B rebuilt index magic/version differs");
            }
            int rawKind = input.readUnsignedByte();
            if (rawKind >= IndexKind.values().length) {
                throw new IllegalArgumentException("M5-B rebuilt index kind is unknown");
            }
            IndexKind kind = IndexKind.values()[rawKind];
            ProtocolCoverage coverage = readCoverage(input);
            Sha256Digest task = readDigest(input);
            Sha256Digest outputIdentity = readDigest(input);
            int count = input.readInt();
            if (count < 0 || count > KafkaCompactionRecordsV1.MAX_RECORDS) {
                throw new IllegalArgumentException("M5-B rebuilt index row count is outside its cap");
            }
            List<Row> rows = new ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                rows.add(new Row(
                        readCoverage(input),
                        input.readInt(),
                        input.readLong(),
                        input.readInt(),
                        input.readLong(),
                        input.readLong(),
                        input.readLong(),
                        input.readShort(),
                        input.readInt(),
                        input.readInt(),
                        input.readInt(),
                        readDigest(input)));
            }
            if (input.read() != -1) {
                throw new IllegalArgumentException("M5-B rebuilt index has trailing bytes");
            }
            KafkaCompactionIndexV1 value = new KafkaCompactionIndexV1(kind, coverage, task, outputIdentity, rows);
            if (!value.encode().equals(encoded)) {
                throw new IllegalArgumentException("M5-B rebuilt index is not canonical");
            }
            return value;
        } catch (IOException error) {
            throw new IllegalArgumentException("invalid M5-B rebuilt index", error);
        }
    }

    private static void writeCoverage(DataOutputStream output, ProtocolCoverage value) throws IOException {
        output.writeByte(value.domain().ordinal());
        output.writeLong(value.inclusiveStart());
        output.writeLong(value.exclusiveEnd());
    }

    private static ProtocolCoverage readCoverage(DataInputStream input) throws IOException {
        int domain = input.readUnsignedByte();
        if (domain >= PositionDomain.values().length) {
            throw new IllegalArgumentException("M5-B rebuilt index position domain is unknown");
        }
        return new ProtocolCoverage(PositionDomain.values()[domain], input.readLong(), input.readLong());
    }

    private static void writeDigest(DataOutputStream output, Sha256Digest value) throws IOException {
        output.write(value.bytes().toByteArray());
    }

    private static Sha256Digest readDigest(DataInputStream input) throws IOException {
        byte[] value = input.readNBytes(Sha256Digest.LENGTH);
        if (value.length != Sha256Digest.LENGTH) {
            throw new EOFException("truncated M5-B index digest");
        }
        return Sha256Digest.copyOf(value);
    }
}
