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
                .sorted((left, right) -> left.coverage().compareStart(right.coverage()))
                .toList();
        if (!rows.equals(sorted)) {
            throw new IllegalArgumentException("M5 lookup index rows are not sorted");
        }
        for (int index = 0; index < rows.size(); index++) {
            Row row = rows.get(index);
            if (!coverage.contains(row.coverage())
                    || row.coverage().domain() != coverage.domain()
                    || (index > 0
                            && (coverage.typedPulsar()
                                    ? !rows.get(index - 1).coverage().adjacentTo(row.coverage())
                                    : rows.get(index - 1).coverage().exclusiveEnd()
                                            > row.coverage().inclusiveStart()))) {
                throw new IllegalArgumentException("M5 lookup index row overlaps or escapes coverage");
            }
        }
        if (coverage.typedPulsar()
                && (rows.isEmpty()
                        || !rows.get(0).coverage().sameStart(coverage)
                        || !rows.get(rows.size() - 1).coverage().sameEnd(coverage)
                        || !List.of(
                                        IndexKind.OFFSET_OR_POSITION,
                                        IndexKind.PAYLOAD_LOCATOR,
                                        IndexKind.TIMESTAMP,
                                        IndexKind.CHECKSUM_COVERAGE)
                                .contains(kind))) {
            throw new IllegalArgumentException(
                    "Pulsar index requires complete per-entry coverage and native index kind");
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
            if (byteOffset < 0 || byteLength < 0 || byteLength == 0 && !coverage.typedPulsar()) {
                throw new IllegalArgumentException("M5 lookup byte range is invalid");
            }
            Math.addExact(byteOffset, byteLength);
            if (coverage.typedPulsar() && coverage.entryCount() != 1) {
                throw new IllegalArgumentException("Pulsar lookup row must locate one original entry");
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
        if (coverage.domain() != PositionDomain.KAFKA_OFFSET) {
            throw new IllegalArgumentException("Pulsar lookup requires a ledger-keyed position");
        }
        if (requested < coverage.inclusiveStart() || requested >= coverage.exclusiveEnd()) {
            return Optional.empty();
        }
        int low = 0;
        int high = rows.size();
        while (low < high) {
            int middle = low + (high - low) / 2;
            if (rows.get(middle).coverage().inclusiveStart() <= requested) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        int floor = low - 1;
        if (floor >= 0 && rows.get(floor).covers(requested)) {
            return Optional.of(rows.get(floor));
        }
        return low < rows.size() ? Optional.of(rows.get(low)) : Optional.empty();
    }

    public Optional<Row> lookup(M5PulsarCoverageV2.Position requested) {
        var scope = coverage.pulsarCoverage()
                .orElseThrow(() -> new IllegalArgumentException("Pulsar lookup lacks a typed captured chain"));
        if (scope.compare(requested, scope.firstPosition()) < 0 || scope.compare(requested, scope.endPosition()) >= 0) {
            return Optional.empty();
        }
        var target = scope.firstAtOrAfter(requested);
        if (target.isEmpty()) {
            return Optional.empty();
        }
        int low = 0;
        int high = rows.size();
        while (low < high) {
            int middle = low + (high - low) / 2;
            var position =
                    rows.get(middle).coverage().pulsarCoverage().orElseThrow().firstPosition();
            if (scope.compare(position, target.orElseThrow()) < 0) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        if (low == rows.size()
                || !rows.get(low)
                        .coverage()
                        .pulsarCoverage()
                        .orElseThrow()
                        .firstPosition()
                        .equals(target.orElseThrow())) {
            throw new IllegalStateException("Pulsar index has no entry for its declared coverage");
        }
        return Optional.of(rows.get(low));
    }

    public CanonicalBytes encode() {
        return encodeBounded(MAX_BYTES);
    }

    public CanonicalBytes encodeBounded(int maximumBytes) {
        if (maximumBytes <= 0 || maximumBytes > MAX_BYTES) {
            throw new IllegalArgumentException("lookup byte allowance exceeds its existing hard cap");
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream() {
                private void requireSpace(int length) {
                    if (length < 0 || Math.addExact(count, length) > maximumBytes) {
                        throw new IllegalArgumentException("M5 lookup index exceeds its reserved byte allowance");
                    }
                }

                @Override
                public synchronized void write(int value) {
                    requireSpace(1);
                    super.write(value);
                }

                @Override
                public synchronized void write(byte[] value, int offset, int length) {
                    requireSpace(length);
                    super.write(value, offset, length);
                }
            };
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(MAGIC);
                output.writeInt(coverage.typedPulsar() ? 2 : VERSION);
                output.writeByte(kind.ordinal());
                writeCoverage(output, coverage);
                writeDigest(output, taskIdSha256);
                writeDigest(output, outputIdentitySha256);
                output.writeInt(rows.size());
                for (Row row : rows) {
                    M5CoverageWireV2.writeChild(output, row.coverage(), coverage);
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
            if (input.readInt() != MAGIC) {
                throw new IllegalArgumentException("M5 lookup index magic differs");
            }
            int version = input.readInt();
            int rawKind = input.readUnsignedByte();
            if (rawKind >= IndexKind.values().length) {
                throw new IllegalArgumentException("M5 lookup index kind is unknown");
            }
            IndexKind kind = IndexKind.values()[rawKind];
            ProtocolCoverage coverage = M5CoverageWireV2.readFull(input, version);
            Sha256Digest task = readDigest(input);
            Sha256Digest output = readDigest(input);
            int count = input.readInt();
            if (count < 0 || count > MAX_ROWS) {
                throw new IllegalArgumentException("M5 lookup index row count is outside its cap");
            }
            java.util.ArrayList<Row> rows = new java.util.ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                rows.add(new Row(
                        M5CoverageWireV2.readChild(input, coverage),
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
        M5CoverageWireV2.writeFull(output, value);
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
