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
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PayloadKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Fixed NMS1 v1 physical projection with strict section caps and a digest-binding footer. */
public final class Nms1CodecV1 {
    public static final int WIRE_VERSION = 1;
    public static final int HEADER_MAGIC = 0x4e4d5331; // NMS1
    public static final int SOURCE_MAGIC = 0x4e535431; // NST1
    public static final int EXTENT_MAGIC = 0x4e455831; // NEX1
    public static final int INDEX_MAGIC = 0x4e495831; // NIX1
    public static final int FOOTER_MAGIC = 0x4e4d4631; // NMF1
    public static final int MAX_HEADER_BYTES = 1024;
    public static final int MAX_SOURCE_TABLE_BYTES = 64 * 1024;
    public static final int MAX_EXTENT_DIRECTORY_BYTES = 8 * 1024 * 1024;
    public static final int MAX_INDEX_DIRECTORY_BYTES = 16 * 1024;
    public static final int MAX_PAYLOAD_BYTES = 256 * 1024 * 1024;
    public static final int MAX_CANONICAL_BODY_BYTES = 384 * 1024 * 1024;
    public static final int FOOTER_BYTES = 4 + 4 + (5 * Sha256Digest.LENGTH) + Long.BYTES;

    private Nms1CodecV1() {}

    public static CanonicalBytes encode(Nms1ObjectV1 value) {
        CanonicalBytes header = encodeSection(out -> writeHeader(out, value), MAX_HEADER_BYTES, "header");
        CanonicalBytes sources =
                encodeSection(out -> writeSources(out, value.sources()), MAX_SOURCE_TABLE_BYTES, "sources");
        CanonicalBytes extents =
                encodeSection(out -> writeExtents(out, value.extents()), MAX_EXTENT_DIRECTORY_BYTES, "extents");
        CanonicalBytes indexDirectory =
                encodeSection(out -> writeIndexDirectory(out, value.indexes()), MAX_INDEX_DIRECTORY_BYTES, "indexes");
        CanonicalBytes indexBodies = concatenate(
                value.indexes().stream().map(Nms1ObjectV1.IndexSection::body).toList());
        long totalLength = Math.addExact(
                Math.addExact(
                        Math.addExact(
                                Math.addExact(header.length(), sources.length()),
                                Math.addExact(extents.length(), indexDirectory.length())),
                        Math.addExact(value.payload().length(), indexBodies.length())),
                FOOTER_BYTES);
        if (totalLength > MAX_CANONICAL_BODY_BYTES) {
            throw new IllegalArgumentException("NMS1 canonical body exceeds its hard cap");
        }
        CanonicalBytes footer = encodeSection(
                out -> {
                    out.writeInt(FOOTER_MAGIC);
                    out.writeInt(WIRE_VERSION);
                    writeDigest(out, Sha256Digest.hash(header));
                    writeDigest(out, Sha256Digest.hash(sources));
                    writeDigest(out, Sha256Digest.hash(extents));
                    writeDigest(out, Sha256Digest.hash(value.payload()));
                    writeDigest(out, Sha256Digest.hash(indexBodies));
                    out.writeLong(totalLength);
                },
                FOOTER_BYTES,
                "footer");
        if (footer.length() != FOOTER_BYTES) {
            throw new IllegalStateException("NMS1 footer projection length differs");
        }
        return concatenate(List.of(header, sources, extents, indexDirectory, value.payload(), indexBodies, footer));
    }

    public static Nms1ObjectV1 decode(CanonicalBytes encoded) {
        if (encoded == null || encoded.length() <= FOOTER_BYTES || encoded.length() > MAX_CANONICAL_BODY_BYTES) {
            throw new IllegalArgumentException("NMS1 canonical body length is outside its cap");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded.toByteArray()))) {
            Header header = readHeader(input);
            List<Nms1ObjectV1.SourceContribution> sources = readSources(input, header.sourceCount());
            List<Nms1ObjectV1.ExtentRow> extents = readExtents(input, header.extentCount());
            List<IndexDescriptor> indexDescriptors = readIndexDirectory(input, header.indexCount());
            CanonicalBytes payload = header.payloadBytes() == 0
                    ? CanonicalBytes.empty()
                    : readExactBytes(input, header.payloadBytes(), MAX_PAYLOAD_BYTES, "payload");
            List<Nms1ObjectV1.IndexSection> indexes = new ArrayList<>(indexDescriptors.size());
            for (IndexDescriptor descriptor : indexDescriptors) {
                CanonicalBytes body =
                        readExactBytes(input, descriptor.bodyBytes(), Nms1ObjectV1.MAX_INDEX_BYTES, "index body");
                indexes.add(new Nms1ObjectV1.IndexSection(
                        descriptor.kind(),
                        descriptor.coverage(),
                        descriptor.parserVersion(),
                        body,
                        descriptor.bodySha256()));
            }
            Footer footer = readFooter(input);
            if (input.read() != -1) {
                throw new IllegalArgumentException("NMS1 body has trailing bytes");
            }
            Nms1ObjectV1 result = new Nms1ObjectV1(
                    header.identity(),
                    header.payloadKind(),
                    header.taskIdSha256(),
                    header.outputIdentitySha256(),
                    header.coverage(),
                    header.partOrdinal(),
                    header.partCount(),
                    header.encryptionGenerationSha256(),
                    header.compressionPolicySha256(),
                    header.checksumPolicySha256(),
                    sources,
                    extents,
                    payload,
                    indexes);
            CanonicalBytes canonical = encode(result);
            if (!canonical.equals(encoded) || footer.totalCanonicalBytes() != encoded.length()) {
                throw new IllegalArgumentException("NMS1 body is not the canonical projection");
            }
            verifyFooter(canonical, result, footer);
            return result;
        } catch (IOException error) {
            throw new IllegalArgumentException("invalid NMS1 canonical body", error);
        }
    }

    public static Sha256Digest bodySha256(Nms1ObjectV1 value) {
        return Sha256Digest.hash(encode(value));
    }

    private static void writeHeader(DataOutputStream out, Nms1ObjectV1 value) throws IOException {
        out.writeInt(HEADER_MAGIC);
        out.writeInt(WIRE_VERSION);
        out.writeByte(value.payloadKind().ordinal());
        writeEnvelope(out, value.identity());
        writeDigest(out, value.taskIdSha256());
        writeDigest(out, value.outputIdentitySha256());
        writeCoverage(out, value.coverage());
        out.writeInt(value.partOrdinal());
        out.writeInt(value.partCount());
        writeDigest(out, value.encryptionGenerationSha256());
        writeDigest(out, value.compressionPolicySha256());
        writeDigest(out, value.checksumPolicySha256());
        out.writeInt(value.sources().size());
        out.writeInt(value.extents().size());
        out.writeInt(value.indexes().size());
        out.writeInt(value.payload().length());
    }

    private static Header readHeader(DataInputStream input) throws IOException {
        if (input.readInt() != HEADER_MAGIC || input.readInt() != WIRE_VERSION) {
            throw new IllegalArgumentException("NMS1 header magic/version differs");
        }
        PayloadKind kind = enumValue(PayloadKind.values(), input.readUnsignedByte(), "payload kind");
        IdentityEnvelope identity = readEnvelope(input);
        Sha256Digest taskId = readDigest(input);
        Sha256Digest outputId = readDigest(input);
        ProtocolCoverage coverage = readCoverage(input);
        int ordinal = input.readInt();
        int partCount = input.readInt();
        Sha256Digest encryption = readDigest(input);
        Sha256Digest compression = readDigest(input);
        Sha256Digest checksum = readDigest(input);
        int sources = boundedCount(input.readInt(), Nms1ObjectV1.MAX_SOURCE_ROWS, "source count", false);
        int extents = boundedCount(input.readInt(), Nms1ObjectV1.MAX_EXTENT_ROWS, "extent count", false);
        int indexes = boundedCount(input.readInt(), Nms1ObjectV1.MAX_INDEX_SECTIONS, "index count", true);
        int payloadBytes = boundedCount(
                input.readInt(), MAX_PAYLOAD_BYTES, "payload bytes", kind == PayloadKind.KAFKA_SEMANTIC_COMPACTED_V1);
        return new Header(
                kind,
                identity,
                taskId,
                outputId,
                coverage,
                ordinal,
                partCount,
                encryption,
                compression,
                checksum,
                sources,
                extents,
                indexes,
                payloadBytes);
    }

    private static void writeSources(DataOutputStream out, List<Nms1ObjectV1.SourceContribution> values)
            throws IOException {
        out.writeInt(SOURCE_MAGIC);
        out.writeInt(WIRE_VERSION);
        for (Nms1ObjectV1.SourceContribution value : values) {
            writeDigest(out, value.sourceIdentitySha256());
            writeCoverage(out, value.coverage());
            writeDigest(out, value.contributedBytesSha256());
        }
    }

    private static List<Nms1ObjectV1.SourceContribution> readSources(DataInputStream input, int count)
            throws IOException {
        requireSectionPreamble(input, SOURCE_MAGIC, "source table");
        List<Nms1ObjectV1.SourceContribution> result = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            result.add(new Nms1ObjectV1.SourceContribution(readDigest(input), readCoverage(input), readDigest(input)));
        }
        return result;
    }

    private static void writeExtents(DataOutputStream out, List<Nms1ObjectV1.ExtentRow> values) throws IOException {
        out.writeInt(EXTENT_MAGIC);
        out.writeInt(WIRE_VERSION);
        for (Nms1ObjectV1.ExtentRow value : values) {
            writeCoverage(out, value.coverage());
            out.writeInt(value.payloadOffset());
            out.writeInt(value.payloadLength());
            out.writeInt(value.recordCount());
            out.writeLong(value.minimumTimestamp());
            out.writeLong(value.maximumTimestamp());
            writeDigest(out, value.payloadSha256());
            out.writeInt(value.protocolFlags());
        }
    }

    private static List<Nms1ObjectV1.ExtentRow> readExtents(DataInputStream input, int count) throws IOException {
        requireSectionPreamble(input, EXTENT_MAGIC, "extent directory");
        List<Nms1ObjectV1.ExtentRow> result = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            result.add(new Nms1ObjectV1.ExtentRow(
                    readCoverage(input),
                    input.readInt(),
                    input.readInt(),
                    input.readInt(),
                    input.readLong(),
                    input.readLong(),
                    readDigest(input),
                    input.readInt()));
        }
        return result;
    }

    private static void writeIndexDirectory(DataOutputStream out, List<Nms1ObjectV1.IndexSection> values)
            throws IOException {
        out.writeInt(INDEX_MAGIC);
        out.writeInt(WIRE_VERSION);
        for (Nms1ObjectV1.IndexSection value : values) {
            out.writeByte(value.kind().ordinal());
            writeCoverage(out, value.coverage());
            out.writeInt(value.parserVersion());
            out.writeInt(value.body().length());
            writeDigest(out, value.bodySha256());
        }
    }

    private static List<IndexDescriptor> readIndexDirectory(DataInputStream input, int count) throws IOException {
        requireSectionPreamble(input, INDEX_MAGIC, "index directory");
        List<IndexDescriptor> result = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            result.add(new IndexDescriptor(
                    enumValue(IndexKind.values(), input.readUnsignedByte(), "index kind"),
                    readCoverage(input),
                    input.readInt(),
                    boundedCount(input.readInt(), Nms1ObjectV1.MAX_INDEX_BYTES, "index bytes", false),
                    readDigest(input)));
        }
        return result;
    }

    private static Footer readFooter(DataInputStream input) throws IOException {
        if (input.readInt() != FOOTER_MAGIC || input.readInt() != WIRE_VERSION) {
            throw new IllegalArgumentException("NMS1 footer magic/version differs");
        }
        return new Footer(
                readDigest(input),
                readDigest(input),
                readDigest(input),
                readDigest(input),
                readDigest(input),
                input.readLong());
    }

    private static void verifyFooter(CanonicalBytes canonical, Nms1ObjectV1 value, Footer footer) {
        CanonicalBytes header = encodeSection(out -> writeHeader(out, value), MAX_HEADER_BYTES, "header");
        CanonicalBytes sources =
                encodeSection(out -> writeSources(out, value.sources()), MAX_SOURCE_TABLE_BYTES, "sources");
        CanonicalBytes extents =
                encodeSection(out -> writeExtents(out, value.extents()), MAX_EXTENT_DIRECTORY_BYTES, "extents");
        CanonicalBytes indexBodies = concatenate(
                value.indexes().stream().map(Nms1ObjectV1.IndexSection::body).toList());
        if (!footer.headerSha256().equals(Sha256Digest.hash(header))
                || !footer.sourcesSha256().equals(Sha256Digest.hash(sources))
                || !footer.extentsSha256().equals(Sha256Digest.hash(extents))
                || !footer.payloadSha256().equals(Sha256Digest.hash(value.payload()))
                || !footer.indexBodiesSha256().equals(Sha256Digest.hash(indexBodies))
                || footer.totalCanonicalBytes() != canonical.length()) {
            throw new IllegalArgumentException("NMS1 footer binding differs");
        }
    }

    private static void writeEnvelope(DataOutputStream out, IdentityEnvelope value) throws IOException {
        writeDigest(out, value.protocolCellSha256());
        writeDigest(out, value.providerScopeSha256());
        writeDigest(out, value.binding().bindingId().digest());
        writeDigest(out, value.binding().incarnationSha256());
        writeDigest(out, value.binding().storageEpochSha256());
        out.writeLong(value.ownerEpoch());
        out.writeLong(value.workerEpoch());
        out.writeLong(value.storageFence());
        out.writeLong(value.capability().generation());
        writeDigest(out, value.capability().evidenceSha256());
    }

    private static IdentityEnvelope readEnvelope(DataInputStream input) throws IOException {
        Sha256Digest cell = readDigest(input);
        Sha256Digest provider = readDigest(input);
        BindingIdentity binding =
                new BindingIdentity(new TopicBindingId(readDigest(input)), readDigest(input), readDigest(input));
        long owner = input.readLong();
        long worker = input.readLong();
        long storage = input.readLong();
        CapabilityBinding capability = new CapabilityBinding(input.readLong(), readDigest(input));
        return new IdentityEnvelope(cell, provider, binding, owner, worker, storage, capability);
    }

    private static void writeCoverage(DataOutputStream out, ProtocolCoverage value) throws IOException {
        out.writeByte(value.domain().ordinal());
        out.writeLong(value.inclusiveStart());
        out.writeLong(value.exclusiveEnd());
    }

    private static ProtocolCoverage readCoverage(DataInputStream input) throws IOException {
        return new ProtocolCoverage(
                enumValue(PositionDomain.values(), input.readUnsignedByte(), "position domain"),
                input.readLong(),
                input.readLong());
    }

    private static void writeDigest(DataOutputStream out, Sha256Digest value) throws IOException {
        out.write(value.bytes().toByteArray());
    }

    private static Sha256Digest readDigest(DataInputStream input) throws IOException {
        byte[] bytes = input.readNBytes(Sha256Digest.LENGTH);
        if (bytes.length != Sha256Digest.LENGTH) {
            throw new EOFException("truncated NMS1 digest");
        }
        return Sha256Digest.copyOf(bytes);
    }

    private static void requireSectionPreamble(DataInputStream input, int magic, String label) throws IOException {
        if (input.readInt() != magic || input.readInt() != WIRE_VERSION) {
            throw new IllegalArgumentException("NMS1 " + label + " magic/version differs");
        }
    }

    private static CanonicalBytes readExactBytes(DataInputStream input, int length, int maximum, String label)
            throws IOException {
        if (length <= 0 || length > maximum) {
            throw new IllegalArgumentException("NMS1 " + label + " length is outside its cap");
        }
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) {
            throw new EOFException("truncated NMS1 " + label);
        }
        return CanonicalBytes.copyOf(bytes);
    }

    private static int boundedCount(int value, int maximum, String label, boolean allowZero) {
        if (value < (allowZero ? 0 : 1) || value > maximum) {
            throw new IllegalArgumentException("NMS1 " + label + " is outside its cap");
        }
        return value;
    }

    private static <T extends Enum<T>> T enumValue(T[] values, int code, String label) {
        if (code < 0 || code >= values.length) {
            throw new IllegalArgumentException("NMS1 " + label + " is unknown: " + code);
        }
        return values[code];
    }

    private static CanonicalBytes concatenate(List<CanonicalBytes> values) {
        long total = 0;
        for (CanonicalBytes value : values) {
            total = Math.addExact(total, value.length());
        }
        if (total > MAX_CANONICAL_BODY_BYTES) {
            throw new IllegalArgumentException("NMS1 concatenated sections exceed the body cap");
        }
        byte[] result = new byte[(int) total];
        int offset = 0;
        for (CanonicalBytes value : values) {
            value.copyTo(result, offset);
            offset += value.length();
        }
        return CanonicalBytes.copyOf(result);
    }

    private static CanonicalBytes encodeSection(Writer writer, int maximum, String label) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writer.write(output);
            }
            if (bytes.size() <= 0 || bytes.size() > maximum) {
                throw new IllegalArgumentException("NMS1 " + label + " exceeds its section cap");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException error) {
            throw new IllegalStateException("in-memory NMS1 encoding failed", error);
        }
    }

    private record Header(
            PayloadKind payloadKind,
            IdentityEnvelope identity,
            Sha256Digest taskIdSha256,
            Sha256Digest outputIdentitySha256,
            ProtocolCoverage coverage,
            int partOrdinal,
            int partCount,
            Sha256Digest encryptionGenerationSha256,
            Sha256Digest compressionPolicySha256,
            Sha256Digest checksumPolicySha256,
            int sourceCount,
            int extentCount,
            int indexCount,
            int payloadBytes) {}

    private record IndexDescriptor(
            IndexKind kind, ProtocolCoverage coverage, int parserVersion, int bodyBytes, Sha256Digest bodySha256) {}

    private record Footer(
            Sha256Digest headerSha256,
            Sha256Digest sourcesSha256,
            Sha256Digest extentsSha256,
            Sha256Digest payloadSha256,
            Sha256Digest indexBodiesSha256,
            long totalCanonicalBytes) {}

    @FunctionalInterface
    private interface Writer {
        void write(DataOutputStream output) throws IOException;
    }
}
