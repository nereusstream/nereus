/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.kafka.checkpoint;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.staging.PrivateStagedObjectFile;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.zip.CRC32C;

/** Strict streaming NKC1 encoder and bounded decoder. */
public final class KafkaCheckpointCodecV1 {
    private static final int HEADER_LENGTH_OFFSET = 4 + Short.BYTES * 2 + Integer.BYTES;

    public EncodedKafkaCheckpoint encodeToStaging(
            StagingFileManager stagingFiles,
            KafkaCheckpointHeader header,
            List<KafkaCheckpointSection> sections) {
        Objects.requireNonNull(stagingFiles, "stagingFiles");
        Objects.requireNonNull(header, "header");
        List<KafkaCheckpointSection> exact = List.copyOf(Objects.requireNonNull(sections, "sections"));
        validateSections(header.flags(), exact);
        byte[] headerBytes = encodeHeader(header, exact.size());
        PrivateStagedObjectFile staged = stagingFiles.create("kafka-checkpoint");
        try {
            OutputStream output = staged.outputStream();
            MessageDigest contentDigest = sha256();
            long contentLength = 0;
            contentLength = writeContent(output, contentDigest, headerBytes, contentLength);
            for (KafkaCheckpointSection section : exact) {
                byte[] payload = section.payload();
                byte[] sectionHeader = encodeSectionHeader(section, payload);
                contentLength = writeContent(output, contentDigest, sectionHeader, contentLength);
                contentLength = writeContent(output, contentDigest, payload, contentLength);
            }
            Checksum contentSha256 = sha256Checksum(contentDigest.digest());
            byte[] trailer = encodeTrailer(contentLength, contentSha256);
            long objectLength = Math.addExact(contentLength, trailer.length);
            if (objectLength > KafkaCheckpointFormatV1.MAX_OBJECT_BYTES) {
                throw new KafkaCheckpointFormatException("NKC1 object exceeds its hard limit");
            }
            output.write(trailer);
            output.close();
            staged.seal();
            if (staged.sealedLength() != objectLength) {
                throw new KafkaCheckpointFormatException("staged NKC1 length changed while encoding");
            }
            return new EncodedKafkaCheckpoint(staged, contentLength, contentSha256);
        } catch (IOException | RuntimeException failure) {
            staged.close();
            if (failure instanceof KafkaCheckpointFormatException exactFailure) throw exactFailure;
            throw new KafkaCheckpointFormatException("failed to encode NKC1", failure);
        }
    }

    public Decoded decode(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length <= KafkaCheckpointFormatV1.TRAILER_BYTES
                || bytes.length > KafkaCheckpointFormatV1.MAX_OBJECT_BYTES) {
            throw new KafkaCheckpointFormatException("NKC1 object length is outside the supported bound");
        }
        try {
            int trailerOffset = bytes.length - KafkaCheckpointFormatV1.TRAILER_BYTES;
            Trailer trailer = decodeTrailer(bytes, trailerOffset);
            if (trailer.contentLength() != trailerOffset) {
                throw malformed("NKC1 trailer content length is not exact");
            }
            Checksum actualContent = sha256(bytes, 0, trailerOffset);
            if (!actualContent.equals(trailer.contentSha256())) {
                throw malformed("NKC1 content SHA-256 mismatch");
            }
            Cursor cursor = new Cursor(bytes, 0, trailerOffset);
            cursor.requireMagic(KafkaCheckpointFormatV1.MAGIC);
            int formatVersion = cursor.readUnsignedShort("formatVersion");
            int minReaderVersion = cursor.readUnsignedShort("minReaderVersion");
            int flags = cursor.readInt("flags");
            int headerLength = cursor.readInt("headerLength");
            if (formatVersion != KafkaCheckpointFormatV1.FORMAT_VERSION
                    || minReaderVersion != KafkaCheckpointFormatV1.MIN_READER_VERSION
                    || (flags & ~KafkaCheckpointFormatV1.HEADER_ALLOW_OPTIONAL_SECTIONS_FLAG) != 0
                    || headerLength < cursor.position()
                    || headerLength > KafkaCheckpointFormatV1.MAX_HEADER_BYTES
                    || headerLength > trailerOffset) {
                throw malformed("unsupported or malformed NKC1 header");
            }
            KafkaCheckpointHeader header = new KafkaCheckpointHeader(
                    flags,
                    cursor.readString("kafkaClusterId", headerLength),
                    KafkaCheckpointFormatV1.topicId(cursor.readBytes("topicId", 16, headerLength)),
                    cursor.readInt("partitionId", headerLength),
                    cursor.readLong("incarnation", headerLength),
                    new com.nereusstream.api.StreamId(cursor.readString("streamId", headerLength)),
                    cursor.readInt("payloadMappingId", headerLength),
                    cursor.readInt("leaderEpoch", headerLength),
                    cursor.readLong("checkpointOffset", headerLength),
                    cursor.readLong("logStartOffset", headerLength),
                    cursor.readLong("stableEndOffset", headerLength),
                    cursor.readLong("sourceCommitVersion", headerLength),
                    cursor.readString("sourceLastCommitId", headerLength),
                    new Checksum(ChecksumType.SHA256,
                            HexFormat.of().formatHex(cursor.readBytes("sourceHeadSha256", 32, headerLength))));
            int sectionCount = cursor.readInt("sectionCount", headerLength);
            if (sectionCount < 0 || sectionCount > KafkaCheckpointFormatV1.MAX_SECTION_COUNT
                    || cursor.position() != headerLength) {
                throw malformed("NKC1 header length or section count is invalid");
            }
            ArrayList<KafkaCheckpointSection> sections = new ArrayList<>(sectionCount);
            for (int index = 0; index < sectionCount; index++) {
                int type = cursor.readUnsignedShort("sectionType");
                int version = cursor.readUnsignedShort("sectionVersion");
                int sectionFlags = cursor.readInt("sectionFlags");
                long payloadLength = cursor.readLong("payloadLength");
                int expectedCrc = cursor.readInt("payloadCrc32c");
                byte[] expectedSha = cursor.readBytes("payloadSha256", 32);
                if (payloadLength < 0 || payloadLength > KafkaCheckpointFormatV1.MAX_SECTION_BYTES
                        || payloadLength > cursor.remaining()) {
                    throw malformed("NKC1 section payload length is invalid");
                }
                byte[] payload = cursor.readBytes("sectionPayload", Math.toIntExact(payloadLength));
                int actualCrc = Crc32cChecksums.intValue(Crc32cChecksums.checksum(payload));
                if (actualCrc != expectedCrc
                        || !Arrays.equals(expectedSha, HexFormat.of().parseHex(sha256(payload).value()))) {
                    throw malformed("NKC1 section checksum mismatch");
                }
                sections.add(new KafkaCheckpointSection(type, version, sectionFlags, payload));
            }
            if (cursor.position() != trailerOffset) {
                throw malformed("NKC1 contains trailing content outside declared sections");
            }
            validateSections(header.flags(), sections);
            return new Decoded(
                    header,
                    sections,
                    Crc32cChecksums.checksum(bytes),
                    sha256(bytes),
                    trailer.contentSha256());
        } catch (KafkaCheckpointFormatException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new KafkaCheckpointFormatException("malformed NKC1", failure);
        }
    }

    private static byte[] encodeHeader(KafkaCheckpointHeader header, int sectionCount) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(bytes);
            output.write(KafkaCheckpointFormatV1.MAGIC);
            output.writeShort(KafkaCheckpointFormatV1.FORMAT_VERSION);
            output.writeShort(KafkaCheckpointFormatV1.MIN_READER_VERSION);
            output.writeInt(header.flags());
            output.writeInt(0);
            writeString(output, header.kafkaClusterId());
            output.write(KafkaCheckpointFormatV1.topicIdBytes(header.topicId()));
            output.writeInt(header.partitionId());
            output.writeLong(header.incarnation());
            writeString(output, header.streamId().value());
            output.writeInt(header.payloadMappingId());
            output.writeInt(header.leaderEpoch());
            output.writeLong(header.checkpointOffset());
            output.writeLong(header.logStartOffset());
            output.writeLong(header.stableEndOffset());
            output.writeLong(header.sourceCommitVersion());
            writeString(output, header.sourceLastCommitId());
            output.write(HexFormat.of().parseHex(header.sourceHeadSha256().value()));
            output.writeInt(sectionCount);
            output.flush();
            byte[] encoded = bytes.toByteArray();
            if (encoded.length > KafkaCheckpointFormatV1.MAX_HEADER_BYTES) {
                throw new KafkaCheckpointFormatException("NKC1 header exceeds its hard limit");
            }
            ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN)
                    .putInt(HEADER_LENGTH_OFFSET, encoded.length);
            return encoded;
        } catch (IOException failure) {
            throw new KafkaCheckpointFormatException("failed to encode NKC1 header", failure);
        }
    }

    private static byte[] encodeSectionHeader(KafkaCheckpointSection section, byte[] payload) {
        ByteBuffer header = ByteBuffer.allocate(KafkaCheckpointFormatV1.SECTION_HEADER_BYTES)
                .order(ByteOrder.BIG_ENDIAN);
        header.putShort((short) section.sectionType());
        header.putShort((short) section.sectionVersion());
        header.putInt(section.sectionFlags());
        header.putLong(payload.length);
        header.putInt(Crc32cChecksums.intValue(Crc32cChecksums.checksum(payload)));
        header.put(HexFormat.of().parseHex(sha256(payload).value()));
        return header.array();
    }

    private static byte[] encodeTrailer(long contentLength, Checksum contentSha256) {
        ByteBuffer trailer = ByteBuffer.allocate(KafkaCheckpointFormatV1.TRAILER_BYTES)
                .order(ByteOrder.BIG_ENDIAN);
        trailer.putLong(contentLength);
        trailer.put(HexFormat.of().parseHex(contentSha256.value()));
        int crc = Crc32cChecksums.intValue(Crc32cChecksums.checksum(
                Arrays.copyOf(trailer.array(), Long.BYTES + 32)));
        trailer.putInt(crc);
        return trailer.array();
    }

    private static Trailer decodeTrailer(byte[] bytes, int offset) {
        ByteBuffer trailer = ByteBuffer.wrap(bytes, offset, KafkaCheckpointFormatV1.TRAILER_BYTES)
                .slice().order(ByteOrder.BIG_ENDIAN);
        long contentLength = trailer.getLong();
        byte[] contentSha = new byte[32];
        trailer.get(contentSha);
        int expectedCrc = trailer.getInt();
        int actualCrc = Crc32cChecksums.intValue(Crc32cChecksums.checksum(
                Arrays.copyOfRange(bytes, offset, offset + Long.BYTES + 32)));
        if (expectedCrc != actualCrc) throw malformed("NKC1 trailer CRC32C mismatch");
        return new Trailer(contentLength, sha256Checksum(contentSha));
    }

    private static void validateSections(int headerFlags, List<KafkaCheckpointSection> sections) {
        if (sections.size() > KafkaCheckpointFormatV1.MAX_SECTION_COUNT) {
            throw new KafkaCheckpointFormatException("NKC1 has too many sections");
        }
        EnumSet<KafkaCheckpointSectionType> required = EnumSet.allOf(KafkaCheckpointSectionType.class);
        Set<Integer> seen = new HashSet<>();
        for (KafkaCheckpointSection section : sections) {
            if (!seen.add(section.sectionType())) {
                throw new KafkaCheckpointFormatException("NKC1 section IDs must be unique");
            }
            KafkaCheckpointSectionType known;
            try {
                known = KafkaCheckpointSectionType.fromWireId(section.sectionType());
            } catch (IllegalArgumentException unknown) {
                if (section.required()
                        || (headerFlags & KafkaCheckpointFormatV1.HEADER_ALLOW_OPTIONAL_SECTIONS_FLAG) == 0) {
                    throw new KafkaCheckpointFormatException("NKC1 contains an unsupported section", unknown);
                }
                continue;
            }
            if (!section.required() || section.sectionVersion() != 1) {
                throw new KafkaCheckpointFormatException("known NKC1 sections must be required V1 sections");
            }
            required.remove(known);
        }
        if (!required.isEmpty()) {
            throw new KafkaCheckpointFormatException("NKC1 is missing required sections: " + required);
        }
    }

    private static long writeContent(
            OutputStream output, MessageDigest digest, byte[] bytes, long current) throws IOException {
        long next = Math.addExact(current, bytes.length);
        if (next > KafkaCheckpointFormatV1.MAX_OBJECT_BYTES - KafkaCheckpointFormatV1.TRAILER_BYTES) {
            throw new KafkaCheckpointFormatException("NKC1 content exceeds its hard limit");
        }
        output.write(bytes);
        digest.update(bytes);
        return next;
    }

    private static void writeString(DataOutputStream output, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > KafkaCheckpointFormatV1.MAX_STRING_BYTES) {
            throw new KafkaCheckpointFormatException("NKC1 string exceeds its hard limit");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static Checksum sha256(byte[] bytes) {
        return sha256(bytes, 0, bytes.length);
    }

    private static Checksum sha256(byte[] bytes, int offset, int length) {
        MessageDigest digest = sha256();
        digest.update(bytes, offset, length);
        return sha256Checksum(digest.digest());
    }

    private static Checksum sha256Checksum(byte[] digest) {
        return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(digest));
    }

    private static KafkaCheckpointFormatException malformed(String message) {
        return new KafkaCheckpointFormatException(message);
    }

    public record Decoded(
            KafkaCheckpointHeader header,
            List<KafkaCheckpointSection> sections,
            Checksum storageCrc32c,
            Checksum objectSha256,
            Checksum contentSha256) {
        public Decoded {
            Objects.requireNonNull(header, "header");
            sections = List.copyOf(sections);
        }
    }

    private record Trailer(long contentLength, Checksum contentSha256) { }

    private static final class Cursor {
        private final byte[] bytes;
        private final int limit;
        private int position;

        private Cursor(byte[] bytes, int position, int limit) {
            this.bytes = bytes;
            this.position = position;
            this.limit = limit;
        }

        int position() { return position; }
        int remaining() { return limit - position; }

        void requireMagic(byte[] expected) {
            if (!Arrays.equals(readBytes("magic", expected.length), expected)) {
                throw malformed("NKC1 magic mismatch");
            }
        }

        int readUnsignedShort(String name) {
            require(Short.BYTES, name, limit);
            int value = Short.toUnsignedInt(ByteBuffer.wrap(bytes, position, Short.BYTES)
                    .order(ByteOrder.BIG_ENDIAN).getShort());
            position += Short.BYTES;
            return value;
        }

        int readInt(String name) { return readInt(name, limit); }

        int readInt(String name, int boundary) {
            require(Integer.BYTES, name, boundary);
            int value = ByteBuffer.wrap(bytes, position, Integer.BYTES).order(ByteOrder.BIG_ENDIAN).getInt();
            position += Integer.BYTES;
            return value;
        }

        long readLong(String name) { return readLong(name, limit); }

        long readLong(String name, int boundary) {
            require(Long.BYTES, name, boundary);
            long value = ByteBuffer.wrap(bytes, position, Long.BYTES).order(ByteOrder.BIG_ENDIAN).getLong();
            position += Long.BYTES;
            return value;
        }

        String readString(String name, int boundary) {
            int length = readInt(name + "Length", boundary);
            if (length < 0 || length > KafkaCheckpointFormatV1.MAX_STRING_BYTES) {
                throw malformed("NKC1 " + name + " length is invalid");
            }
            byte[] value = readBytes(name, length, boundary);
            try {
                String decoded = StandardCharsets.UTF_8.newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(value)).toString();
                if (!Arrays.equals(decoded.getBytes(StandardCharsets.UTF_8), value)) {
                    throw malformed("NKC1 " + name + " is not canonical UTF-8");
                }
                return decoded;
            } catch (CharacterCodingException failure) {
                throw new KafkaCheckpointFormatException("NKC1 " + name + " is not strict UTF-8", failure);
            }
        }

        byte[] readBytes(String name, int length) { return readBytes(name, length, limit); }

        byte[] readBytes(String name, int length, int boundary) {
            if (length < 0) throw malformed("negative NKC1 " + name + " length");
            require(length, name, boundary);
            byte[] value = Arrays.copyOfRange(bytes, position, position + length);
            position += length;
            return value;
        }

        private void require(int length, String name, int boundary) {
            if (boundary < position || boundary > limit || length > boundary - position) {
                throw malformed("truncated NKC1 " + name);
            }
        }
    }
}
