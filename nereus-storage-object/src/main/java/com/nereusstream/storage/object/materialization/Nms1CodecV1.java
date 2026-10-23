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
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
    public static final int MAX_PULSAR_HEADER_BYTES = 65_536;
    public static final int MAX_PULSAR_INDEX_DIRECTORY_BYTES = 262_144;
    public static final int MAX_SOURCE_TABLE_BYTES = 64 * 1024;
    public static final int MAX_EXTENT_DIRECTORY_BYTES = 8 * 1024 * 1024;
    public static final int MAX_INDEX_DIRECTORY_BYTES = 16 * 1024;
    public static final int MAX_PAYLOAD_BYTES = 256 * 1024 * 1024;
    public static final int MAX_CANONICAL_BODY_BYTES = 384 * 1024 * 1024;
    public static final int FOOTER_BYTES = 4 + 4 + (5 * Sha256Digest.LENGTH) + Long.BYTES;

    private Nms1CodecV1() {}

    public static CanonicalBytes encode(Nms1ObjectV1 value) {
        CanonicalBytes header = encodeSection(out -> writeHeader(out, value), headerCap(value.coverage()), "header");
        CanonicalBytes sources = encodeSection(
                out -> writeSources(out, value.sources(), value.coverage()), MAX_SOURCE_TABLE_BYTES, "sources");
        CanonicalBytes extents = encodeSection(
                out -> writeExtents(out, value.extents(), value.coverage()), MAX_EXTENT_DIRECTORY_BYTES, "extents");
        CanonicalBytes indexDirectory = encodeSection(
                out -> writeIndexDirectory(out, value.indexes(), value.coverage()),
                indexDirectoryCap(value.coverage()),
                "indexes");
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
                    out.writeInt(M5CoverageWireV2.version(value.coverage()));
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
            List<Nms1ObjectV1.SourceContribution> sources = readSources(input, header.sourceCount(), header.coverage());
            List<Nms1ObjectV1.ExtentRow> extents = readExtents(input, header.extentCount(), header.coverage());
            List<IndexDescriptor> indexDescriptors = readIndexDirectory(input, header.indexCount(), header.coverage());
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
            Footer footer = readFooter(input, M5CoverageWireV2.version(header.coverage()));
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

    public record StreamedBody(long bytes, Sha256Digest sha256) {}

    public static final class VerifiedPulsarPart {
        private final StreamedBody body;
        private final List<M5LookupIndexV1.Row> locators;

        private VerifiedPulsarPart(StreamedBody body, List<M5LookupIndexV1.Row> locators) {
            this.body = body;
            this.locators = List.copyOf(locators);
        }

        public StreamedBody body() {
            return body;
        }

        public List<M5LookupIndexV1.Row> locators() {
            return locators;
        }
    }

    /** Independent rewrite verification against an original cursor; the caller qualifies and owns both I/O sources. */
    public static VerifiedPulsarPart verifyPulsarPart(
            M5MaterializationRecordsV1.MaterializationPlan plan,
            int ordinal,
            InputStream body,
            M5PulsarEntriesV2.Cursor originals)
            throws IOException {
        var part = new PulsarPartInput(plan, ordinal, body);
        var commitment = new M5PulsarEntriesV2.Commitment(
                part.source.coverage().pulsarCoverage().orElseThrow());
        List<M5LookupIndexV1.Row> locators = new ArrayList<>(part.extents.size());
        byte[] buffer = new byte[65_536];
        for (var extent : part.extents) {
            var original = originals.next().orElseThrow(() -> new IOException("Pulsar original entry is missing"));
            commitment.accept(original);
            byte[] expected = original.payload().toByteArray();
            if (!original.position()
                            .equals(extent.coverage()
                                    .pulsarCoverage()
                                    .orElseThrow()
                                    .firstPosition())
                    || expected.length != extent.payloadLength()
                    || original.timestamp() != extent.minimumTimestamp()
                    || original.timestamp() != extent.maximumTimestamp()
                    || !Sha256Digest.hash(original.payload()).equals(extent.payloadSha256())) {
                throw new IOException("Pulsar streamed entry position/length/timestamp/digest differs");
            }
            int offset = 0;
            while (offset != expected.length) {
                int length = Math.min(buffer.length, expected.length - offset);
                part.input.readFully(buffer, 0, length);
                for (int at = 0; at < length; at++) {
                    if (buffer[at] != expected[offset + at]) {
                        throw new IOException("Pulsar streamed entry bytes differ from original");
                    }
                }
                offset += length;
            }
            locators.add(new M5LookupIndexV1.Row(
                    extent.coverage(),
                    ordinal,
                    extent.payloadOffset(),
                    extent.payloadLength(),
                    original.timestamp(),
                    original.timestamp(),
                    extent.payloadSha256()));
        }
        if (originals.next().isPresent()
                || !commitment
                        .finish()
                        .equals(part.source.pulsarEntryRootSha256().orElseThrow())) {
            throw new IOException("Pulsar streamed source entry root/count differs");
        }
        commitment.requireTimestampSummary(part.source.minimumTimestamp(), part.source.maximumTimestamp());
        return new VerifiedPulsarPart(part.finish(), locators);
    }

    /** One bounded owned entry after complete part/body/entry-root verification; grants no Native read authority. */
    public static final class VerifiedPulsarEntry {
        private final M5PulsarEntriesV2.Entry entry;
        private final StreamedBody body;

        private VerifiedPulsarEntry(M5PulsarEntriesV2.Entry entry, StreamedBody body) {
            this.entry = entry;
            this.body = body;
        }

        public M5PulsarEntriesV2.Entry entry() {
            return entry;
        }

        public StreamedBody body() {
            return body;
        }
    }

    /**
     * Reads one exact native position while draining and hashing every entry. Unrequested payloads use one fixed
     * buffer; the requested allocation is checked against the caller's admitted cap before it is made.
     */
    public static VerifiedPulsarEntry readPulsarEntry(
            M5MaterializationRecordsV1.MaterializationPlan plan,
            int ordinal,
            InputStream body,
            M5PulsarCoverageV2.Position requested,
            long maximumEntryBytes)
            throws IOException {
        if (!plan.sourceCut().coverage().typedPulsar()
                || maximumEntryBytes <= 0
                || maximumEntryBytes > Integer.MAX_VALUE
                || ordinal < 0
                || ordinal >= plan.outputParts().size()
                || !plan.outputParts().get(ordinal).coverage().pulsarCoverage().orElseThrow().ranges().stream()
                        .anyMatch(range -> range.ledgerId() == requested.ledgerId()
                                && requested.entryId() >= range.startEntryInclusive()
                                && requested.entryId() < range.endEntryExclusive())) {
            throw new IOException("Pulsar mapped entry/cap is outside its exact planned part");
        }
        var part = new PulsarPartInput(plan, ordinal, body);
        var commitment = new M5PulsarEntriesV2.Commitment(
                part.source.coverage().pulsarCoverage().orElseThrow());
        byte[] buffer = new byte[65_536];
        M5PulsarEntriesV2.Entry selected = null;
        for (var extent : part.extents) {
            var position = extent.coverage().pulsarCoverage().orElseThrow().firstPosition();
            boolean wanted = position.equals(requested);
            if (extent.minimumTimestamp() != extent.maximumTimestamp()
                    || wanted && extent.payloadLength() > maximumEntryBytes) {
                throw new IOException("Pulsar mapped entry timestamp or admitted byte cap differs");
            }
            byte[] payload = wanted ? new byte[extent.payloadLength()] : null;
            var digest = sha256Digest();
            int offset = 0;
            while (offset != extent.payloadLength()) {
                int length = Math.min(buffer.length, extent.payloadLength() - offset);
                part.input.readFully(buffer, 0, length);
                digest.update(buffer, 0, length);
                if (wanted) {
                    System.arraycopy(buffer, 0, payload, offset, length);
                }
                offset += length;
            }
            var measured = Sha256Digest.copyOf(digest.digest());
            if (!measured.equals(extent.payloadSha256())) {
                throw new IOException("Pulsar mapped entry bytes differ from the original entry digest");
            }
            commitment.acceptDigest(position, extent.payloadLength(), measured, extent.minimumTimestamp());
            if (wanted) {
                selected = new M5PulsarEntriesV2.Entry(
                        position, CanonicalBytes.copyOf(payload), extent.minimumTimestamp());
            }
        }
        if (selected == null
                || !commitment
                        .finish()
                        .equals(part.source.pulsarEntryRootSha256().orElseThrow())) {
            throw new IOException("Pulsar mapped source entry root/count differs");
        }
        commitment.requireTimestampSummary(part.source.minimumTimestamp(), part.source.maximumTimestamp());
        return new VerifiedPulsarEntry(selected, part.finish());
    }

    /** The same exact header, directory and footer checks serve independent original comparison and located reads. */
    private static final class PulsarPartInput {
        private final M5MaterializationRecordsV1.SourceExtent source;
        private final MessageDigest fullSha = sha256Digest();
        private final MessageDigest sectionSha = sha256Digest();
        private final CountedInput counted;
        private final DataInputStream input;
        private final Sha256Digest headerSha;
        private final Sha256Digest sourcesSha;
        private final Sha256Digest extentsSha;
        private final List<Nms1ObjectV1.ExtentRow> extents;

        private PulsarPartInput(M5MaterializationRecordsV1.MaterializationPlan plan, int ordinal, InputStream body)
                throws IOException {
            M5MaterializationCodecV1.encodePlan(plan);
            if (!plan.sourceCut().coverage().typedPulsar()
                    || plan.representationMode() != M5MaterializationRecordsV1.RepresentationMode.REWRITE_GENERATION
                    || plan.payloadKind() != PayloadKind.PULSAR_ENTRY_PRESERVING_V1
                    || ordinal < 0
                    || ordinal >= plan.outputParts().size()) {
                throw new IOException("Pulsar streamed verification requires its exact rewrite plan");
            }
            source = plan.sourceCut().sources().get(ordinal);
            counted = new CountedInput(body, fullSha);
            input = new DataInputStream(new java.security.DigestInputStream(counted, sectionSha));
            Header header = readHeader(input);
            if (counted.bytes > MAX_PULSAR_HEADER_BYTES) {
                throw new IOException("Pulsar streamed header exceeds its byte cap");
            }
            requireStreamedHeader(plan, ordinal, header);
            headerSha = Sha256Digest.copyOf(sectionSha.digest());
            var contributions = readSources(input, header.sourceCount(), header.coverage());
            if (!contributions.equals(List.of(new Nms1ObjectV1.SourceContribution(
                    source.sourceIdentitySha256(),
                    source.coverage(),
                    source.pulsarEntryRootSha256().orElseThrow())))) {
                throw new IOException("Pulsar streamed source contribution differs");
            }
            sourcesSha = Sha256Digest.copyOf(sectionSha.digest());
            long directoryStart = counted.bytes;
            extents = readExtents(input, header.extentCount(), header.coverage());
            if (counted.bytes - directoryStart > MAX_EXTENT_DIRECTORY_BYTES) {
                throw new IOException("Pulsar streamed directory exceeds its byte cap");
            }
            requirePulsarStreamExtents(header.coverage(), extents, header.payloadBytes());
            extentsSha = Sha256Digest.copyOf(sectionSha.digest());
            readIndexDirectory(input, 0, header.coverage());
            sectionSha.digest();
        }

        private StreamedBody finish() throws IOException {
            var payloadSha = Sha256Digest.copyOf(sectionSha.digest());
            Footer footer = readFooter(input, 2);
            if (!footer.headerSha256().equals(headerSha)
                    || !footer.sourcesSha256().equals(sourcesSha)
                    || !footer.extentsSha256().equals(extentsSha)
                    || !footer.payloadSha256().equals(payloadSha)
                    || !footer.indexBodiesSha256().equals(Sha256Digest.hash(CanonicalBytes.empty()))
                    || footer.totalCanonicalBytes() != counted.bytes
                    || input.read() != -1) {
                throw new IOException("Pulsar streamed footer/length/EOF differs");
            }
            return new StreamedBody(counted.bytes, Sha256Digest.copyOf(fullSha.digest()));
        }
    }

    private static void requireStreamedHeader(
            M5MaterializationRecordsV1.MaterializationPlan plan, int ordinal, Header value) throws IOException {
        if (!value.identity().equals(plan.sourceCut().identity())
                || value.payloadKind() != plan.payloadKind()
                || !value.taskIdSha256().equals(plan.taskIdSha256())
                || !value.outputIdentitySha256().equals(plan.outputIdentitySha256())
                || !value.coverage().equals(plan.outputParts().get(ordinal).coverage())
                || value.partOrdinal() != ordinal
                || value.partCount() != plan.outputParts().size()
                || !value.encryptionGenerationSha256().equals(plan.encryptionGenerationSha256())
                || !value.compressionPolicySha256().equals(plan.compressionPolicySha256())
                || !value.checksumPolicySha256().equals(plan.checksumPolicySha256())
                || value.sourceCount() != 1
                || value.indexCount() != 0) {
            throw new IOException("Pulsar streamed header differs from its exact external-index plan");
        }
    }

    private static final class CountedInput extends InputStream {
        private final InputStream original;
        private final MessageDigest digest;
        private long bytes;

        private CountedInput(InputStream original, MessageDigest digest) {
            this.original = original;
            this.digest = digest;
        }

        @Override
        public int read() throws IOException {
            int value = original.read();
            if (value != -1) {
                requireCount(1);
                digest.update((byte) value);
            }
            return value;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            int count = original.read(buffer, offset, length);
            if (count > 0) {
                requireCount(count);
                digest.update(buffer, offset, count);
            }
            return count;
        }

        private void requireCount(int count) throws IOException {
            bytes = Math.addExact(bytes, count);
            if (bytes > MAX_CANONICAL_BODY_BYTES) {
                throw new IOException("Pulsar streamed Object exceeds its canonical byte cap");
            }
        }
    }

    /** Exact Pulsar rewrite projection; payload is copied with one fixed64KiB buffer, never a part-sized array. */
    public static StreamedBody writePulsarPart(
            M5MaterializationRecordsV1.MaterializationPlan plan,
            int ordinal,
            List<Nms1ObjectV1.ExtentRow> extents,
            long payloadBytes,
            Sha256Digest payloadSha256,
            InputStream payload,
            OutputStream target)
            throws IOException {
        M5MaterializationCodecV1.encodePlan(plan);
        if (!plan.sourceCut().coverage().typedPulsar()
                || plan.payloadKind() != PayloadKind.PULSAR_ENTRY_PRESERVING_V1
                || plan.representationMode() != M5MaterializationRecordsV1.RepresentationMode.REWRITE_GENERATION
                || ordinal < 0
                || ordinal >= plan.outputParts().size()
                || payloadBytes < 0
                || payloadBytes > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("streamed Pulsar part differs from its rewrite plan");
        }
        var source = plan.sourceCut().sources().get(ordinal);
        var part = plan.outputParts().get(ordinal);
        extents = List.copyOf(extents);
        requirePulsarStreamExtents(part.coverage(), extents, payloadBytes);
        int entries = extents.size();
        CanonicalBytes header = encodeSection(
                out -> {
                    out.writeInt(HEADER_MAGIC);
                    out.writeInt(2);
                    out.writeByte(PayloadKind.PULSAR_ENTRY_PRESERVING_V1.ordinal());
                    writeEnvelope(out, plan.sourceCut().identity());
                    writeDigest(out, plan.taskIdSha256());
                    writeDigest(out, plan.outputIdentitySha256());
                    writeCoverage(out, part.coverage());
                    out.writeInt(ordinal);
                    out.writeInt(plan.outputParts().size());
                    writeDigest(out, plan.encryptionGenerationSha256());
                    writeDigest(out, plan.compressionPolicySha256());
                    writeDigest(out, plan.checksumPolicySha256());
                    out.writeInt(1);
                    out.writeInt(entries);
                    out.writeInt(0);
                    out.writeInt(Math.toIntExact(payloadBytes));
                },
                MAX_PULSAR_HEADER_BYTES,
                "Pulsar header");
        var contributions = List.of(new Nms1ObjectV1.SourceContribution(
                source.sourceIdentitySha256(),
                source.coverage(),
                source.pulsarEntryRootSha256().orElseThrow()));
        CanonicalBytes sources = encodeSection(
                out -> writeSources(out, contributions, part.coverage()), MAX_SOURCE_TABLE_BYTES, "Pulsar sources");
        var exactExtents = extents;
        CanonicalBytes directory = encodeSection(
                out -> writeExtents(out, exactExtents, part.coverage()), MAX_EXTENT_DIRECTORY_BYTES, "Pulsar extents");
        CanonicalBytes indexes = encodeSection(
                out -> writeIndexDirectory(out, List.of(), part.coverage()),
                MAX_PULSAR_INDEX_DIRECTORY_BYTES,
                "Pulsar indexes");
        long total = Math.addExact(
                Math.addExact(
                        Math.addExact(header.length(), sources.length()),
                        Math.addExact(directory.length(), indexes.length())),
                Math.addExact(payloadBytes, FOOTER_BYTES));
        if (total > MAX_CANONICAL_BODY_BYTES) {
            throw new IOException("streamed Pulsar body exceeds its canonical cap");
        }
        MessageDigest bodyDigest = sha256Digest();
        var output = new DigestOutputStream(target, bodyDigest);
        output.write(header.toByteArray());
        output.write(sources.toByteArray());
        output.write(directory.toByteArray());
        output.write(indexes.toByteArray());
        MessageDigest payloadDigest = sha256Digest();
        byte[] buffer = new byte[65_536];
        long remaining = payloadBytes;
        while (remaining != 0) {
            int count = payload.read(buffer, 0, (int) Math.min(buffer.length, remaining));
            if (count <= 0) {
                throw new IOException("streamed Pulsar payload is truncated or stalled");
            }
            payloadDigest.update(buffer, 0, count);
            output.write(buffer, 0, count);
            remaining -= count;
        }
        if (payload.read() != -1 || !Sha256Digest.copyOf(payloadDigest.digest()).equals(payloadSha256)) {
            throw new IOException("streamed Pulsar payload length/digest changed");
        }
        var footer = new DataOutputStream(output);
        footer.writeInt(FOOTER_MAGIC);
        footer.writeInt(2);
        writeDigest(footer, Sha256Digest.hash(header));
        writeDigest(footer, Sha256Digest.hash(sources));
        writeDigest(footer, Sha256Digest.hash(directory));
        writeDigest(footer, payloadSha256);
        writeDigest(footer, Sha256Digest.hash(CanonicalBytes.empty()));
        footer.writeLong(total);
        footer.flush();
        return new StreamedBody(total, Sha256Digest.copyOf(bodyDigest.digest()));
    }

    private static void requirePulsarStreamExtents(
            ProtocolCoverage coverage, List<Nms1ObjectV1.ExtentRow> extents, long payloadBytes) {
        if (extents.isEmpty()
                || extents.size() > Nms1ObjectV1.MAX_EXTENT_ROWS
                || !extents.get(0).coverage().sameStart(coverage)
                || !extents.get(extents.size() - 1).coverage().sameEnd(coverage)) {
            throw new IllegalArgumentException("streamed Pulsar extent coverage is incomplete");
        }
        long offset = 0;
        for (int index = 0; index < extents.size(); index++) {
            var extent = extents.get(index);
            if (!coverage.contains(extent.coverage())
                    || extent.payloadOffset() != offset
                    || extent.recordCount() != 1
                    || extent.coverage().entryCount() != 1
                    || index > 0 && !extents.get(index - 1).coverage().adjacentTo(extent.coverage())) {
                throw new IllegalArgumentException(
                        "streamed Pulsar entry directory differs from exact bytes/positions");
            }
            offset = Math.addExact(offset, extent.payloadLength());
        }
        if (offset != payloadBytes) {
            throw new IllegalArgumentException("streamed Pulsar directory payload length differs");
        }
    }

    private static MessageDigest sha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("JDK has no SHA-256", impossible);
        }
    }

    private static void writeHeader(DataOutputStream out, Nms1ObjectV1 value) throws IOException {
        out.writeInt(HEADER_MAGIC);
        out.writeInt(M5CoverageWireV2.version(value.coverage()));
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
        if (input.readInt() != HEADER_MAGIC) {
            throw new IllegalArgumentException("NMS1 header magic differs");
        }
        int version = input.readInt();
        if (version != 1 && version != 2) {
            throw new IllegalArgumentException("NMS1 header version is unknown");
        }
        PayloadKind kind = enumValue(PayloadKind.values(), input.readUnsignedByte(), "payload kind");
        IdentityEnvelope identity = readEnvelope(input);
        Sha256Digest taskId = readDigest(input);
        Sha256Digest outputId = readDigest(input);
        ProtocolCoverage coverage = M5CoverageWireV2.readFull(input, version);
        int ordinal = input.readInt();
        int partCount = input.readInt();
        Sha256Digest encryption = readDigest(input);
        Sha256Digest compression = readDigest(input);
        Sha256Digest checksum = readDigest(input);
        int sources = boundedCount(input.readInt(), Nms1ObjectV1.MAX_SOURCE_ROWS, "source count", false);
        int extents = boundedCount(input.readInt(), Nms1ObjectV1.MAX_EXTENT_ROWS, "extent count", false);
        int indexes = boundedCount(input.readInt(), Nms1ObjectV1.MAX_INDEX_SECTIONS, "index count", true);
        int payloadBytes = boundedCount(
                input.readInt(),
                MAX_PAYLOAD_BYTES,
                "payload bytes",
                kind == PayloadKind.KAFKA_SEMANTIC_COMPACTED_V1 || coverage.typedPulsar());
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

    private static void writeSources(
            DataOutputStream out, List<Nms1ObjectV1.SourceContribution> values, ProtocolCoverage context)
            throws IOException {
        out.writeInt(SOURCE_MAGIC);
        out.writeInt(M5CoverageWireV2.version(context));
        for (Nms1ObjectV1.SourceContribution value : values) {
            writeDigest(out, value.sourceIdentitySha256());
            M5CoverageWireV2.writeChild(out, value.coverage(), context);
            writeDigest(out, value.contributedBytesSha256());
        }
    }

    private static List<Nms1ObjectV1.SourceContribution> readSources(
            DataInputStream input, int count, ProtocolCoverage context) throws IOException {
        requireSectionPreamble(input, SOURCE_MAGIC, M5CoverageWireV2.version(context), "source table");
        List<Nms1ObjectV1.SourceContribution> result = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            result.add(new Nms1ObjectV1.SourceContribution(
                    readDigest(input), M5CoverageWireV2.readChild(input, context), readDigest(input)));
        }
        return result;
    }

    private static void writeExtents(
            DataOutputStream out, List<Nms1ObjectV1.ExtentRow> values, ProtocolCoverage context) throws IOException {
        out.writeInt(EXTENT_MAGIC);
        out.writeInt(M5CoverageWireV2.version(context));
        for (Nms1ObjectV1.ExtentRow value : values) {
            M5CoverageWireV2.writeChild(out, value.coverage(), context);
            out.writeInt(value.payloadOffset());
            out.writeInt(value.payloadLength());
            out.writeInt(value.recordCount());
            out.writeLong(value.minimumTimestamp());
            out.writeLong(value.maximumTimestamp());
            writeDigest(out, value.payloadSha256());
            out.writeInt(value.protocolFlags());
        }
    }

    private static List<Nms1ObjectV1.ExtentRow> readExtents(DataInputStream input, int count, ProtocolCoverage context)
            throws IOException {
        requireSectionPreamble(input, EXTENT_MAGIC, M5CoverageWireV2.version(context), "extent directory");
        List<Nms1ObjectV1.ExtentRow> result = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            result.add(new Nms1ObjectV1.ExtentRow(
                    M5CoverageWireV2.readChild(input, context),
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

    private static void writeIndexDirectory(
            DataOutputStream out, List<Nms1ObjectV1.IndexSection> values, ProtocolCoverage context) throws IOException {
        out.writeInt(INDEX_MAGIC);
        out.writeInt(M5CoverageWireV2.version(context));
        for (Nms1ObjectV1.IndexSection value : values) {
            out.writeByte(value.kind().ordinal());
            M5CoverageWireV2.writeChild(out, value.coverage(), context);
            out.writeInt(value.parserVersion());
            out.writeInt(value.body().length());
            writeDigest(out, value.bodySha256());
        }
    }

    private static List<IndexDescriptor> readIndexDirectory(DataInputStream input, int count, ProtocolCoverage context)
            throws IOException {
        requireSectionPreamble(input, INDEX_MAGIC, M5CoverageWireV2.version(context), "index directory");
        List<IndexDescriptor> result = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            result.add(new IndexDescriptor(
                    enumValue(IndexKind.values(), input.readUnsignedByte(), "index kind"),
                    M5CoverageWireV2.readChild(input, context),
                    input.readInt(),
                    boundedCount(input.readInt(), Nms1ObjectV1.MAX_INDEX_BYTES, "index bytes", false),
                    readDigest(input)));
        }
        return result;
    }

    private static Footer readFooter(DataInputStream input, int version) throws IOException {
        if (input.readInt() != FOOTER_MAGIC || input.readInt() != version) {
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
        CanonicalBytes header = encodeSection(out -> writeHeader(out, value), headerCap(value.coverage()), "header");
        CanonicalBytes sources = encodeSection(
                out -> writeSources(out, value.sources(), value.coverage()), MAX_SOURCE_TABLE_BYTES, "sources");
        CanonicalBytes extents = encodeSection(
                out -> writeExtents(out, value.extents(), value.coverage()), MAX_EXTENT_DIRECTORY_BYTES, "extents");
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
        M5CoverageWireV2.writeFull(out, value);
    }

    private static int headerCap(ProtocolCoverage coverage) {
        return coverage.typedPulsar() ? MAX_PULSAR_HEADER_BYTES : MAX_HEADER_BYTES;
    }

    private static int indexDirectoryCap(ProtocolCoverage coverage) {
        return coverage.typedPulsar() ? MAX_PULSAR_INDEX_DIRECTORY_BYTES : MAX_INDEX_DIRECTORY_BYTES;
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

    private static void requireSectionPreamble(DataInputStream input, int magic, int version, String label)
            throws IOException {
        if (input.readInt() != magic || input.readInt() != version) {
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
