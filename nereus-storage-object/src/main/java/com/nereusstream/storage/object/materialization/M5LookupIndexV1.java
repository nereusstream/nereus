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

package com.nereusstream.storage.object.materialization;

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
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Canonical bounded lookup/index body shared by M5 materialization and compaction. */
public record M5LookupIndexV1(
        IndexKind kind,
        ProtocolCoverage coverage,
        Sha256Digest taskIdSha256,
        Sha256Digest outputIdentitySha256,
        List<Row> rows) {
    private static final int MAGIC = 0x4d354931; // M5I1
    private static final int VERSION = 1;
    public static final int MAX_ROWS = 1_048_576;
    public static final int MAX_BYTES = 64 * 1024 * 1024;

    public M5LookupIndexV1 {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(coverage, "coverage");
        requireDigest(taskIdSha256, "taskIdSha256");
        requireDigest(outputIdentitySha256, "outputIdentitySha256");
        rows = List.copyOf(Objects.requireNonNull(rows, "rows"));
        if (rows.size() > MAX_ROWS) {
            throw new IllegalArgumentException("M5 lookup index row count exceeds its hard cap");
        }
        List<Row> sorted = rows.stream()
                .sorted(Comparator.comparingLong(value -> value.coverage().inclusiveStart()))
                .toList();
        if (!rows.equals(sorted)) {
            throw new IllegalArgumentException("M5 lookup index rows are not sorted");
        }
        for (int index = 0; index < rows.size(); index++) {
            Row row = rows.get(index);
            if (!coverage.contains(row.coverage())
                    || row.coverage().domain() != coverage.domain()
                    || (index > 0
                            && rows.get(index - 1).coverage().exclusiveEnd()
                                    > row.coverage().inclusiveStart())) {
                throw new IllegalArgumentException("M5 lookup index row overlaps or escapes coverage");
            }
        }
    }

    public record Row(
            ProtocolCoverage coverage,
            int objectOrdinal,
            long byteOffset,
            int byteLength,
            long minimumTimestamp,
            long maximumTimestamp,
            Sha256Digest payloadUnitSha256) {
        public Row {
            Objects.requireNonNull(coverage, "coverage");
            if (objectOrdinal < 0 || objectOrdinal >= M5MaterializationRecordsV1.MAX_PARTS) {
                throw new IllegalArgumentException("M5 lookup Object ordinal is outside its cap");
            }
            if (byteOffset < 0 || byteLength <= 0) {
                throw new IllegalArgumentException("M5 lookup byte range is invalid");
            }
            if (minimumTimestamp < -1 || maximumTimestamp < minimumTimestamp) {
                throw new IllegalArgumentException("M5 lookup timestamp range is invalid");
            }
            requireDigest(payloadUnitSha256, "payloadUnitSha256");
        }

        public boolean covers(long position) {
            return coverage.inclusiveStart() <= position && position < coverage.exclusiveEnd();
        }
    }

    /** Floor, coverage, then successor. Empty means end-of-index for this exact captured bound. */
    public Optional<Row> lookup(long requested) {
        if (requested < coverage.inclusiveStart() || requested >= coverage.exclusiveEnd()) {
            return Optional.empty();
        }
        Row floor = null;
        for (Row row : rows) {
            if (row.coverage().inclusiveStart() <= requested) {
                floor = row;
                continue;
            }
            return floor != null && floor.covers(requested) ? Optional.of(floor) : Optional.of(row);
        }
        return floor != null && floor.covers(requested) ? Optional.of(floor) : Optional.empty();
    }

    public CanonicalBytes encode() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(MAGIC);
                output.writeInt(VERSION);
                output.writeByte(kind.ordinal());
                writeCoverage(output, coverage);
                writeDigest(output, taskIdSha256);
                writeDigest(output, outputIdentitySha256);
                output.writeInt(rows.size());
                for (Row row : rows) {
                    writeCoverage(output, row.coverage());
                    output.writeInt(row.objectOrdinal());
                    output.writeLong(row.byteOffset());
                    output.writeInt(row.byteLength());
                    output.writeLong(row.minimumTimestamp());
                    output.writeLong(row.maximumTimestamp());
                    writeDigest(output, row.payloadUnitSha256());
                }
            }
            if (bytes.size() <= 0 || bytes.size() > MAX_BYTES) {
                throw new IllegalArgumentException("M5 lookup index body exceeds its hard cap");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException error) {
            throw new IllegalStateException("in-memory M5 index encoding failed", error);
        }
    }

    public static M5LookupIndexV1 decode(CanonicalBytes encoded) {
        if (encoded == null || encoded.isEmpty() || encoded.length() > MAX_BYTES) {
            throw new IllegalArgumentException("M5 lookup index body length is outside its cap");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded.toByteArray()))) {
            if (input.readInt() != MAGIC || input.readInt() != VERSION) {
                throw new IllegalArgumentException("M5 lookup index magic/version differs");
            }
            int rawKind = input.readUnsignedByte();
            if (rawKind >= IndexKind.values().length) {
                throw new IllegalArgumentException("M5 lookup index kind is unknown");
            }
            IndexKind kind = IndexKind.values()[rawKind];
            ProtocolCoverage coverage = readCoverage(input);
            Sha256Digest task = readDigest(input);
            Sha256Digest output = readDigest(input);
            int count = input.readInt();
            if (count < 0 || count > MAX_ROWS) {
                throw new IllegalArgumentException("M5 lookup index row count is outside its cap");
            }
            java.util.ArrayList<Row> rows = new java.util.ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                rows.add(new Row(
                        readCoverage(input),
                        input.readInt(),
                        input.readLong(),
                        input.readInt(),
                        input.readLong(),
                        input.readLong(),
                        readDigest(input)));
            }
            if (input.read() != -1) {
                throw new IllegalArgumentException("M5 lookup index has trailing bytes");
            }
            M5LookupIndexV1 result = new M5LookupIndexV1(kind, coverage, task, output, rows);
            if (!result.encode().equals(encoded)) {
                throw new IllegalArgumentException("M5 lookup index is not canonical");
            }
            return result;
        } catch (IOException error) {
            throw new IllegalArgumentException("invalid M5 lookup index body", error);
        }
    }

    private static void writeCoverage(DataOutputStream output, ProtocolCoverage value) throws IOException {
        output.writeByte(value.domain().ordinal());
        output.writeLong(value.inclusiveStart());
        output.writeLong(value.exclusiveEnd());
    }

    private static ProtocolCoverage readCoverage(DataInputStream input) throws IOException {
        int raw = input.readUnsignedByte();
        if (raw >= PositionDomain.values().length) {
            throw new IllegalArgumentException("M5 lookup position domain is unknown");
        }
        return new ProtocolCoverage(PositionDomain.values()[raw], input.readLong(), input.readLong());
    }

    private static void writeDigest(DataOutputStream output, Sha256Digest value) throws IOException {
        output.write(value.bytes().toByteArray());
    }

    private static Sha256Digest readDigest(DataInputStream input) throws IOException {
        byte[] bytes = input.readNBytes(Sha256Digest.LENGTH);
        if (bytes.length != Sha256Digest.LENGTH) {
            throw new EOFException("truncated M5 lookup digest");
        }
        return Sha256Digest.copyOf(bytes);
    }

    private static void requireDigest(Sha256Digest value, String label) {
        Objects.requireNonNull(value, label);
        if (value.isZero()) {
            throw new IllegalArgumentException(label + " is the zero digest");
        }
    }
}
