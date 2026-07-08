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

package io.nereus.objectstore.wal;

import io.nereus.api.Checksum;
import io.nereus.api.ChecksumType;
import io.nereus.api.ObjectId;
import io.nereus.api.ObjectType;
import io.nereus.api.PayloadFormat;
import io.nereus.api.SchemaRef;
import io.nereus.api.StreamId;
import io.nereus.objectstore.Crc32cChecksums;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public final class WalObjectLayout {
    static final byte[] MAGIC = new byte[] {'N', 'R', 'S', '1'};
    static final int FORMAT_MAJOR = 1;
    static final int FORMAT_MINOR = 0;
    static final int COMMON_HEADER_LENGTH = 48;
    static final int SECTION_HEADER_LENGTH = 16;
    static final int OBJECT_TYPE_MULTI_STREAM_WAL = 1;
    static final int SECTION_WAL_OBJECT_HEADER = 1;
    static final int SECTION_STREAM_SLICE_DIRECTORY = 2;
    static final int SECTION_PAYLOAD_BLOCK = 3;
    static final int SECTION_ENTRY_INDEX = 4;
    static final int SECTION_FOOTER = 5;
    static final int CHECKSUM_ZERO_OFFSET = 36;
    static final int OBJECT_CHECKSUM_OFFSET = 40;

    private WalObjectLayout() {
    }

    public record Section(int type, byte[] payload) {
        public Section {
            Objects.requireNonNull(payload, "payload");
        }
    }

    public record EncodedObject(byte[] bytes, Checksum objectChecksum, Checksum storageChecksum) {
        public EncodedObject {
            bytes = Objects.requireNonNull(bytes, "bytes").clone();
            Objects.requireNonNull(objectChecksum, "objectChecksum");
            Objects.requireNonNull(storageChecksum, "storageChecksum");
        }

        @Override
        public byte[] bytes() {
            return bytes.clone();
        }
    }

    public record DecodedObject(
            ObjectId objectId,
            long objectLength,
            Checksum objectChecksum,
            List<StreamSliceDescriptor> slices) {
        public DecodedObject {
            Objects.requireNonNull(objectId, "objectId");
            Objects.requireNonNull(objectChecksum, "objectChecksum");
            slices = List.copyOf(Objects.requireNonNull(slices, "slices"));
            if (objectLength <= 0) {
                throw new IllegalArgumentException("objectLength must be positive");
            }
        }
    }

    static EncodedObject encodeObject(
            List<Section> sections,
            long footerOffset,
            int footerLength) {
        Objects.requireNonNull(sections, "sections");
        int objectLength = checkedIntLength(footerOffset, footerLength);
        byte[] bytes = assemble(sections, footerOffset, footerLength, 0, 0, objectLength);
        int headerChecksum = headerChecksum(bytes);
        bytes = assemble(sections, footerOffset, footerLength, headerChecksum, 0, objectLength);
        int objectChecksum = objectChecksum(bytes);
        bytes = assemble(sections, footerOffset, footerLength, headerChecksum, objectChecksum, objectLength);
        return new EncodedObject(
                bytes,
                Crc32cChecksums.checksum(objectChecksum),
                Crc32cChecksums.checksum(bytes));
    }

    public static DecodedObject decode(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        try {
            return decodeInternal(bytes);
        } catch (io.nereus.api.NereusException e) {
            throw e;
        } catch (IllegalArgumentException | ArithmeticException e) {
            throw WalBinary.corrupt("invalid WAL object metadata", e);
        }
    }

    private static DecodedObject decodeInternal(byte[] bytes) {
        if (bytes.length < COMMON_HEADER_LENGTH) {
            throw WalBinary.corrupt("object is smaller than common header");
        }
        ByteBuffer header = ByteBuffer.wrap(bytes, 0, COMMON_HEADER_LENGTH).order(ByteOrder.LITTLE_ENDIAN);
        byte[] magic = new byte[MAGIC.length];
        header.get(magic);
        if (!Arrays.equals(magic, MAGIC)) {
            throw WalBinary.corrupt("invalid WAL object magic");
        }
        int major = header.getInt();
        int minor = header.getInt();
        int objectType = header.getInt();
        header.getInt(); // flags
        int headerLength = header.getInt();
        long footerOffset = header.getLong();
        int footerLength = header.getInt();
        int storedHeaderChecksum = header.getInt();
        int storedObjectChecksum = header.getInt();
        int encryptionLength = header.getInt();
        if (major != FORMAT_MAJOR || minor != FORMAT_MINOR) {
            throw WalBinary.corrupt("unsupported WAL object format version");
        }
        if (objectType != OBJECT_TYPE_MULTI_STREAM_WAL) {
            throw WalBinary.corrupt("unsupported WAL object type");
        }
        if (headerLength != COMMON_HEADER_LENGTH || encryptionLength != 0) {
            throw WalBinary.corrupt("unsupported WAL object header shape");
        }
        if (footerOffset < COMMON_HEADER_LENGTH || footerLength <= SECTION_HEADER_LENGTH
                || footerOffset > bytes.length || footerLength > bytes.length - footerOffset) {
            throw WalBinary.corrupt("footer bounds exceed object length");
        }
        if (storedHeaderChecksum != headerChecksum(bytes)) {
            throw WalBinary.corrupt("common header checksum mismatch");
        }
        if (storedObjectChecksum != objectChecksum(bytes)) {
            throw WalBinary.corrupt("WAL object checksum mismatch");
        }

        List<SectionWithOffset> sections = readSections(bytes);
        SectionWithOffset footer = sections.stream()
                .filter(section -> section.type() == SECTION_FOOTER)
                .findFirst()
                .orElseThrow(() -> WalBinary.corrupt("missing footer section"));
        if (footer.offset() != footerOffset || footer.encodedLength() != footerLength) {
            throw WalBinary.corrupt("footer offset or length mismatch");
        }
        WalBinary.Reader footerReader = new WalBinary.Reader(footer.payload());
        int footerChecksum = footerReader.int32();
        byte[] footerPayload = footer.payload().clone();
        putInt(footerPayload, 0, 0);
        if (footerChecksum != Crc32cChecksums.intValue(Crc32cChecksums.checksum(footerPayload))) {
            throw WalBinary.corrupt("footer checksum mismatch");
        }
        ObjectId objectId = new ObjectId(footerReader.string());
        int sliceCount = footerReader.int32();
        footerReader.int64(); // entryIndexDirectoryOffset reserved for future use
        footerReader.int64(); // entryIndexDirectoryLength reserved for future use
        List<StreamSliceDescriptor> descriptors = new ArrayList<>();
        for (int i = 0; i < sliceCount; i++) {
            StreamSliceDescriptor descriptor = decodeDescriptor(footerReader);
            validateDescriptorBounds(descriptor, bytes.length);
            descriptors.add(descriptor);
        }
        footerReader.requireFullyConsumed();
        return new DecodedObject(
                objectId,
                bytes.length,
                Crc32cChecksums.checksum(storedObjectChecksum),
                descriptors);
    }

    static byte[] encodeWalObjectHeader(
            ObjectId objectId,
            String cluster,
            String writerId,
            String writerRunIdHash,
            long writerEpoch,
            String writerVersion,
            long createdAtMillis,
            CompressionType compression,
            int streamSliceCount,
            int payloadBlockCount,
            long minEventTimeMillis,
            long maxEventTimeMillis) {
        WalBinary.Writer writer = new WalBinary.Writer();
        writer.string(objectId.value());
        writer.string(cluster);
        writer.string(writerId);
        writer.string(writerRunIdHash);
        writer.int64(writerEpoch);
        writer.string(writerVersion);
        writer.int64(createdAtMillis);
        writer.int32(compression == CompressionType.NONE ? 0 : 1);
        writer.int32(streamSliceCount);
        writer.int32(payloadBlockCount);
        writer.int64(minEventTimeMillis);
        writer.int64(maxEventTimeMillis);
        return writer.toByteArray();
    }

    static byte[] encodeSliceDirectory(List<StreamSliceDescriptor> descriptors) {
        WalBinary.Writer writer = new WalBinary.Writer();
        writer.int32(descriptors.size());
        for (StreamSliceDescriptor descriptor : descriptors) {
            encodeDescriptor(writer, descriptor);
        }
        return writer.toByteArray();
    }

    static byte[] encodeFooter(ObjectId objectId, List<StreamSliceDescriptor> descriptors) {
        WalBinary.Writer writer = new WalBinary.Writer();
        writer.int32(0);
        writer.string(objectId.value());
        writer.int32(descriptors.size());
        writer.int64(0);
        writer.int64(0);
        for (StreamSliceDescriptor descriptor : descriptors) {
            encodeDescriptor(writer, descriptor);
        }
        byte[] payload = writer.toByteArray();
        int checksum = Crc32cChecksums.intValue(Crc32cChecksums.checksum(payload));
        putInt(payload, 0, checksum);
        return payload;
    }

    static byte[] encodeSection(int sectionType, byte[] payload) {
        byte[] section = new byte[Math.addExact(SECTION_HEADER_LENGTH, payload.length)];
        putInt(section, 0, sectionType);
        putInt(section, 4, 1);
        putInt(section, 8, payload.length);
        putInt(section, 12, Crc32cChecksums.intValue(Crc32cChecksums.checksum(payload)));
        System.arraycopy(payload, 0, section, SECTION_HEADER_LENGTH, payload.length);
        return section;
    }

    static int sectionEncodedLength(byte[] payload) {
        return Math.addExact(SECTION_HEADER_LENGTH, payload.length);
    }

    private static byte[] assemble(
            List<Section> sections,
            long footerOffset,
            int footerLength,
            int headerChecksum,
            int objectChecksum,
            int objectLength) {
        byte[] bytes = new byte[objectLength];
        putCommonHeader(bytes, footerOffset, footerLength, headerChecksum, objectChecksum);
        int offset = COMMON_HEADER_LENGTH;
        for (Section section : sections) {
            byte[] encodedSection = encodeSection(section.type(), section.payload());
            System.arraycopy(encodedSection, 0, bytes, offset, encodedSection.length);
            offset = Math.addExact(offset, encodedSection.length);
        }
        if (offset != objectLength) {
            throw new IllegalArgumentException("section layout length does not match object length");
        }
        return bytes;
    }

    private static int checkedIntLength(long footerOffset, int footerLength) {
        long length = Math.addExact(footerOffset, footerLength);
        if (length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("object length exceeds in-memory encoder limit");
        }
        return (int) length;
    }

    private static void putCommonHeader(
            byte[] bytes,
            long footerOffset,
            int footerLength,
            int headerChecksum,
            int objectChecksum) {
        System.arraycopy(MAGIC, 0, bytes, 0, MAGIC.length);
        putInt(bytes, 4, FORMAT_MAJOR);
        putInt(bytes, 8, FORMAT_MINOR);
        putInt(bytes, 12, OBJECT_TYPE_MULTI_STREAM_WAL);
        putInt(bytes, 16, 0);
        putInt(bytes, 20, COMMON_HEADER_LENGTH);
        putLong(bytes, 24, footerOffset);
        putInt(bytes, 32, footerLength);
        putInt(bytes, CHECKSUM_ZERO_OFFSET, headerChecksum);
        putInt(bytes, OBJECT_CHECKSUM_OFFSET, objectChecksum);
        putInt(bytes, 44, 0);
    }

    private static int headerChecksum(byte[] bytes) {
        byte[] header = Arrays.copyOfRange(bytes, 0, COMMON_HEADER_LENGTH);
        putInt(header, CHECKSUM_ZERO_OFFSET, 0);
        putInt(header, OBJECT_CHECKSUM_OFFSET, 0);
        return Crc32cChecksums.intValue(Crc32cChecksums.checksum(header));
    }

    private static int objectChecksum(byte[] bytes) {
        byte[] copy = bytes.clone();
        putInt(copy, OBJECT_CHECKSUM_OFFSET, 0);
        return Crc32cChecksums.intValue(Crc32cChecksums.checksum(copy));
    }

    private static List<SectionWithOffset> readSections(byte[] bytes) {
        List<SectionWithOffset> sections = new ArrayList<>();
        int offset = COMMON_HEADER_LENGTH;
        while (offset < bytes.length) {
            if (bytes.length - offset < SECTION_HEADER_LENGTH) {
                throw WalBinary.corrupt("truncated section header");
            }
            ByteBuffer sectionHeader = ByteBuffer.wrap(bytes, offset, SECTION_HEADER_LENGTH).order(ByteOrder.LITTLE_ENDIAN);
            int type = sectionHeader.getInt();
            int version = sectionHeader.getInt();
            int length = sectionHeader.getInt();
            int checksum = sectionHeader.getInt();
            if (version != 1 || length < 0 || length > bytes.length - offset - SECTION_HEADER_LENGTH) {
                throw WalBinary.corrupt("invalid section header");
            }
            byte[] payload = Arrays.copyOfRange(
                    bytes,
                    offset + SECTION_HEADER_LENGTH,
                    offset + SECTION_HEADER_LENGTH + length);
            if (checksum != Crc32cChecksums.intValue(Crc32cChecksums.checksum(payload))) {
                throw WalBinary.corrupt("section checksum mismatch");
            }
            sections.add(new SectionWithOffset(offset, type, payload));
            offset = Math.addExact(offset, SECTION_HEADER_LENGTH + length);
        }
        return sections;
    }

    private static void encodeDescriptor(WalBinary.Writer writer, StreamSliceDescriptor descriptor) {
        writer.int32(descriptor.sliceOrdinal());
        writer.string(descriptor.streamId().value());
        writer.string(descriptor.sliceId());
        writer.int64(descriptor.writerEpoch());
        writer.int64(descriptor.relativeBaseOffset());
        writer.int32(descriptor.entryCount());
        writer.int32(descriptor.recordCount());
        writer.int64(descriptor.logicalBytes());
        writer.int64(descriptor.payloadOffset());
        writer.int64(descriptor.payloadLength());
        writer.int64(descriptor.entryIndexOffset());
        writer.int64(descriptor.entryIndexLength());
        writer.string(descriptor.checksum().type().name());
        writer.string(descriptor.checksum().value());
        writer.string(descriptor.payloadFormat().name());
        writer.int64(descriptor.minEventTimeMillis());
        writer.int64(descriptor.maxEventTimeMillis());
        writer.int32(descriptor.schemaRefs().size());
        for (SchemaRef schemaRef : descriptor.schemaRefs()) {
            writer.string(schemaRef.namespace());
            writer.string(schemaRef.id());
            writer.int64(schemaRef.version());
        }
    }

    private static StreamSliceDescriptor decodeDescriptor(WalBinary.Reader reader) {
        int sliceOrdinal = reader.int32();
        StreamId streamId = new StreamId(reader.string());
        String sliceId = reader.string();
        long writerEpoch = reader.int64();
        long relativeBaseOffset = reader.int64();
        int entryCount = reader.int32();
        int recordCount = reader.int32();
        long logicalBytes = reader.int64();
        long payloadOffset = reader.int64();
        long payloadLength = reader.int64();
        long entryIndexOffset = reader.int64();
        long entryIndexLength = reader.int64();
        Checksum checksum = new Checksum(ChecksumType.valueOf(reader.string()), reader.string());
        PayloadFormat payloadFormat = PayloadFormat.valueOf(reader.string());
        long minEventTimeMillis = reader.int64();
        long maxEventTimeMillis = reader.int64();
        int schemaRefCount = reader.int32();
        if (schemaRefCount < 0) {
            throw WalBinary.corrupt("negative schema ref count");
        }
        List<SchemaRef> schemaRefs = new ArrayList<>();
        for (int i = 0; i < schemaRefCount; i++) {
            schemaRefs.add(new SchemaRef(reader.string(), reader.string(), reader.int64()));
        }
        return new StreamSliceDescriptor(
                sliceOrdinal,
                streamId,
                sliceId,
                writerEpoch,
                relativeBaseOffset,
                entryCount,
                recordCount,
                logicalBytes,
                payloadOffset,
                payloadLength,
                entryIndexOffset,
                entryIndexLength,
                checksum,
                payloadFormat,
                minEventTimeMillis,
                maxEventTimeMillis,
                schemaRefs);
    }

    private static void validateDescriptorBounds(StreamSliceDescriptor descriptor, long objectLength) {
        if (descriptor.payloadOffset() > objectLength
                || descriptor.payloadLength() > objectLength - descriptor.payloadOffset()
                || descriptor.entryIndexOffset() > objectLength
                || descriptor.entryIndexLength() > objectLength - descriptor.entryIndexOffset()) {
            throw WalBinary.corrupt("slice descriptor range exceeds object length");
        }
    }

    private static void putInt(byte[] bytes, int offset, int value) {
        ByteBuffer.wrap(bytes, offset, Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(value);
    }

    private static void putLong(byte[] bytes, int offset, long value) {
        ByteBuffer.wrap(bytes, offset, Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(value);
    }

    private record SectionWithOffset(int offset, int type, byte[] payload) {
        int encodedLength() {
            return SECTION_HEADER_LENGTH + payload.length;
        }
    }
}
