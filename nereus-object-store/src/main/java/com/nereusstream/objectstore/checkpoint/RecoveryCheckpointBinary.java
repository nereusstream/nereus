/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.StreamId;
import com.nereusstream.objectstore.Crc32cChecksums;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32C;

final class RecoveryCheckpointBinary {
    private RecoveryCheckpointBinary() {
    }

    static byte[] encodeHeader(RecoveryCheckpointWriteRequest value) {
        Objects.requireNonNull(value, "value");
        return withCrc(writer -> {
            writer.raw(RecoveryCheckpointFormatV1.HEADER_MAGIC);
            writer.unsignedShort(RecoveryCheckpointFormatV1.MAJOR_VERSION);
            writer.unsignedShort(RecoveryCheckpointFormatV1.FLAGS);
            writer.string(value.cluster(), "cluster");
            writer.string(value.streamId().value(), "streamId");
            writer.int64(value.checkpointSequence());
            writer.string(value.checkpointAttemptId(), "checkpointAttemptId");
            writer.int64(value.coverage().startOffset());
            writer.int64(value.coverage().endOffset());
            writer.int64(value.firstCommitVersion());
            writer.int64(value.lastCommitVersion());
            writer.int64(value.cumulativeSizeAtStart());
            writer.int64(value.cumulativeSizeAtEnd());
            writer.string(value.firstCommitId(), "firstCommitId");
            writer.string(value.lastCommitId(), "lastCommitId");
            writer.string(value.sourceHeadCommitId(), "sourceHeadCommitId");
            writer.int64(value.sourceHeadCommitVersion());
            writer.checksum(value.projectionIdentitySha256());
            writer.int32(value.expectedEntryCount());
            writer.int32(value.expectedPublicationCount());
        });
    }

    static Decoded<RecoveryCheckpointWriteRequest> decodeHeader(ByteBuffer bytes) {
        Reader reader = new Reader(bytes, "NRC1 header");
        try {
            reader.magic(RecoveryCheckpointFormatV1.HEADER_MAGIC, "NRC1 header");
            if (reader.unsignedShort("majorVersion") != RecoveryCheckpointFormatV1.MAJOR_VERSION
                    || reader.unsignedShort("flags") != RecoveryCheckpointFormatV1.FLAGS) {
                throw corrupt("unsupported NRC1 version or flags");
            }
            RecoveryCheckpointWriteRequest value = new RecoveryCheckpointWriteRequest(
                    reader.string("cluster"),
                    new StreamId(reader.string("streamId")),
                    reader.int64("checkpointSequence"),
                    reader.string("checkpointAttemptId"),
                    new OffsetRange(reader.int64("coveredStartOffset"), reader.int64("coveredEndOffset")),
                    reader.int64("firstCommitVersion"),
                    reader.int64("lastCommitVersion"),
                    reader.int64("cumulativeSizeAtStart"),
                    reader.int64("cumulativeSizeAtEnd"),
                    reader.string("firstCommitId"),
                    reader.string("lastCommitId"),
                    reader.string("sourceHeadCommitId"),
                    reader.int64("sourceHeadCommitVersion"),
                    reader.sha256("projectionIdentitySha256"),
                    reader.int32("entryCount"),
                    reader.int32("publicationCount"));
            reader.verifyTrailingCrc("NRC1 header");
            if (reader.consumed() > RecoveryCheckpointFormatV1.MAX_HEADER_BYTES) {
                throw corrupt("NRC1 header exceeds its hard limit");
            }
            return new Decoded<>(value, reader.consumed());
        } catch (RecoveryCheckpointFormatException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw corrupt("invalid NRC1 header", failure);
        }
    }

    static byte[] encodePublication(RecoveryCheckpointPublication value) {
        Objects.requireNonNull(value, "value");
        return withCrc(writer -> {
            writer.int64(value.generation());
            writer.string(value.publicationId().value(), "publicationId");
            writer.int64(value.coverage().startOffset());
            writer.int64(value.coverage().endOffset());
            writer.bytes(value.canonicalGenerationIndexRecord(), "canonicalGenerationIndexRecord");
            writer.checksum(value.generationIndexRecordSha256());
        });
    }

    static Decoded<RecoveryCheckpointPublication> decodePublication(ByteBuffer bytes) {
        Reader reader = new Reader(bytes, "NRC1 publication");
        try {
            RecoveryCheckpointPublication value = new RecoveryCheckpointPublication(
                    reader.int64("generation"),
                    new PublicationId(reader.string("publicationId")),
                    new OffsetRange(reader.int64("coverageStart"), reader.int64("coverageEnd")),
                    reader.byteBuffer("canonicalGenerationIndexRecord"),
                    reader.sha256("generationIndexRecordSha256"));
            reader.verifyTrailingCrc("NRC1 publication");
            requireRecordLimit(reader.consumed(), "NRC1 publication");
            return new Decoded<>(value, reader.consumed());
        } catch (RecoveryCheckpointFormatException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw corrupt("invalid NRC1 publication", failure);
        }
    }

    static byte[] encodeEntry(RecoveryCheckpointEntry value) {
        Objects.requireNonNull(value, "value");
        return withCrc(writer -> {
            writer.int64(value.commitVersion());
            writer.int64(value.range().startOffset());
            writer.int64(value.range().endOffset());
            writer.int64(value.cumulativeSizeAtEnd());
            writer.string(value.commitId(), "commitId");
            writer.string(value.previousCommitId(), "previousCommitId");
            writer.bytes(value.canonicalCommitRecord(), "canonicalCommitRecord");
            writer.checksum(value.canonicalCommitRecordSha256());
            writer.int32(value.coveringPublicationIndexes().size());
            for (int index : value.coveringPublicationIndexes()) {
                writer.int32(index);
            }
        });
    }

    static Decoded<RecoveryCheckpointEntry> decodeEntry(ByteBuffer bytes) {
        Reader reader = new Reader(bytes, "NRC1 commit entry");
        try {
            long commitVersion = reader.int64("commitVersion");
            OffsetRange range = new OffsetRange(
                    reader.int64("offsetStart"), reader.int64("offsetEnd"));
            long cumulativeSizeAtEnd = reader.int64("cumulativeSizeAtEnd");
            String commitId = reader.string("commitId");
            String previousCommitId = reader.string("previousCommitId");
            ByteBuffer canonical = reader.byteBuffer("canonicalCommitRecord");
            Checksum canonicalSha256 = reader.sha256("canonicalCommitRecordSha256");
            int count = reader.int32("coveringPublicationCount");
            if (count < 0 || count > RecoveryCheckpointFormatV1.MAX_PUBLICATION_REFS_PER_ENTRY) {
                throw corrupt("NRC1 commit entry publication-reference count exceeds its hard limit");
            }
            List<Integer> indexes = new ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                indexes.add(reader.int32("publicationTableIndex"));
            }
            RecoveryCheckpointEntry value = new RecoveryCheckpointEntry(
                    commitVersion,
                    range,
                    cumulativeSizeAtEnd,
                    commitId,
                    previousCommitId,
                    canonical,
                    canonicalSha256,
                    indexes);
            reader.verifyTrailingCrc("NRC1 commit entry");
            requireRecordLimit(reader.consumed(), "NRC1 commit entry");
            return new Decoded<>(value, reader.consumed());
        } catch (RecoveryCheckpointFormatException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw corrupt("invalid NRC1 commit entry", failure);
        }
    }

    static byte[] encodeFooter(RecoveryCheckpointDirectory directory, Checksum bodySha256) {
        Objects.requireNonNull(directory, "directory");
        RecoveryCheckpointValidation.requireSha256(bodySha256, "bodySha256");
        byte[] encoded = withCrc(writer -> {
            writer.raw(RecoveryCheckpointFormatV1.FOOTER_MAGIC);
            writer.int64(directory.publicationDirectoryOffset());
            writer.int64(directory.publicationDirectoryLength());
            writer.int64(directory.commitDirectoryOffset());
            writer.int64(directory.commitDirectoryLength());
            writer.checksum(bodySha256);
        });
        if (encoded.length != RecoveryCheckpointFormatV1.FOOTER_BYTES) {
            throw new IllegalStateException("NRC1 footer length is not fixed");
        }
        return encoded;
    }

    static Footer decodeFooter(ByteBuffer bytes) {
        Reader reader = new Reader(bytes, "NRC1 footer");
        try {
            reader.magic(RecoveryCheckpointFormatV1.FOOTER_MAGIC, "NRC1 footer");
            long publicationOffset = reader.int64("publicationDirectoryOffset");
            long publicationLength = reader.int64("publicationDirectoryLength");
            long commitOffset = reader.int64("commitDirectoryOffset");
            long commitLength = reader.int64("commitDirectoryLength");
            Checksum bodySha256 = reader.sha256("bodySha256");
            reader.verifyTrailingCrc("NRC1 footer");
            if (reader.consumed() != RecoveryCheckpointFormatV1.FOOTER_BYTES
                    || reader.remaining() != 0) {
                throw corrupt("NRC1 footer has an invalid fixed length");
            }
            return new Footer(
                    new RecoveryCheckpointDirectory(
                            publicationOffset,
                            publicationLength,
                            commitOffset,
                            commitLength,
                            RecoveryCheckpointFormatV1.COMMIT_DIRECTORY_STRIDE),
                    bodySha256);
        } catch (RecoveryCheckpointFormatException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw corrupt("invalid NRC1 footer", failure);
        }
    }

    private static byte[] withCrc(WriterAction action) {
        Writer writer = new Writer();
        action.write(writer);
        byte[] body = writer.toByteArray();
        Writer complete = new Writer();
        complete.raw(body);
        complete.int32(Crc32cChecksums.intValue(Crc32cChecksums.checksum(body)));
        return complete.toByteArray();
    }

    private static void requireRecordLimit(int bytes, String field) {
        if (bytes > RecoveryCheckpointFormatV1.MAX_RECORD_BYTES) {
            throw corrupt(field + " exceeds its hard limit");
        }
    }

    private static RecoveryCheckpointFormatException corrupt(String message) {
        return new RecoveryCheckpointFormatException(message);
    }

    private static RecoveryCheckpointFormatException corrupt(String message, Throwable cause) {
        return new RecoveryCheckpointFormatException(message, cause);
    }

    record Decoded<T>(T value, int bytesConsumed) {
    }

    record Footer(RecoveryCheckpointDirectory directory, Checksum bodySha256) {
    }

    @FunctionalInterface
    private interface WriterAction {
        void write(Writer writer);
    }

    private static final class Writer {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private final DataOutputStream output = new DataOutputStream(bytes);

        private void unsignedShort(int value) {
            if (value < 0 || value > 0xffff) {
                throw new IllegalArgumentException("unsigned short is out of range");
            }
            io(() -> output.writeShort(value));
        }

        private void int32(int value) {
            io(() -> output.writeInt(value));
        }

        private void int64(long value) {
            io(() -> output.writeLong(value));
        }

        private void string(String value, String field) {
            byte[] encoded = RecoveryCheckpointValidation.strictUtf8(value, field);
            if (encoded.length > RecoveryCheckpointFormatV1.MAX_STRING_BYTES) {
                throw new IllegalArgumentException(field + " exceeds the NRC1 string limit");
            }
            int32(encoded.length);
            raw(encoded);
        }

        private void bytes(ByteBuffer value, String field) {
            ByteBuffer source = Objects.requireNonNull(value, field).asReadOnlyBuffer();
            if (!source.hasRemaining()
                    || source.remaining() > RecoveryCheckpointFormatV1.MAX_EMBEDDED_RECORD_BYTES) {
                throw new IllegalArgumentException(field + " exceeds the NRC1 embedded-record limit");
            }
            byte[] copied = new byte[source.remaining()];
            source.get(copied);
            int32(copied.length);
            raw(copied);
        }

        private void checksum(Checksum value) {
            RecoveryCheckpointValidation.requireSha256(value, "checksum");
            raw(HexFormat.of().parseHex(value.value()));
        }

        private void raw(byte[] value) {
            io(() -> output.write(value));
        }

        private byte[] toByteArray() {
            return bytes.toByteArray();
        }

        private static void io(IoAction action) {
            try {
                action.run();
            } catch (IOException impossible) {
                throw new IllegalStateException("in-memory NRC1 encoding failed", impossible);
            }
        }
    }

    private static final class Reader {
        private final ByteBuffer source;
        private final int start;
        private final String context;

        private Reader(ByteBuffer value, String context) {
            this.source = Objects.requireNonNull(value, "value").asReadOnlyBuffer().order(ByteOrder.BIG_ENDIAN);
            this.start = source.position();
            this.context = context;
        }

        private void magic(byte[] expected, String field) {
            require(expected.length, field);
            for (byte value : expected) {
                if (source.get() != value) {
                    throw corrupt(field + " magic mismatch");
                }
            }
        }

        private int unsignedShort(String field) {
            require(Short.BYTES, field);
            return Short.toUnsignedInt(source.getShort());
        }

        private int int32(String field) {
            require(Integer.BYTES, field);
            return source.getInt();
        }

        private long int64(String field) {
            require(Long.BYTES, field);
            return source.getLong();
        }

        private String string(String field) {
            int length = int32(field + "Length");
            if (length < 0 || length > RecoveryCheckpointFormatV1.MAX_STRING_BYTES) {
                throw corrupt(field + " length exceeds the NRC1 string limit");
            }
            require(length, field);
            ByteBuffer encoded = source.slice().order(ByteOrder.BIG_ENDIAN);
            encoded.limit(length);
            source.position(source.position() + length);
            try {
                return StandardCharsets.UTF_8.newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(encoded)
                        .toString();
            } catch (CharacterCodingException failure) {
                throw corrupt(field + " is malformed UTF-8", failure);
            }
        }

        private ByteBuffer byteBuffer(String field) {
            int length = int32(field + "Length");
            if (length <= 0 || length > RecoveryCheckpointFormatV1.MAX_EMBEDDED_RECORD_BYTES) {
                throw corrupt(field + " length exceeds the NRC1 embedded-record limit");
            }
            require(length, field);
            ByteBuffer result = source.slice().order(ByteOrder.BIG_ENDIAN);
            result.limit(length);
            source.position(source.position() + length);
            return result.asReadOnlyBuffer();
        }

        private Checksum sha256(String field) {
            require(32, field);
            byte[] value = new byte[32];
            source.get(value);
            return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(value));
        }

        private void verifyTrailingCrc(String field) {
            int crcPosition = source.position();
            int expected = int32(field + "Crc32c");
            ByteBuffer protectedBytes = source.duplicate();
            protectedBytes.position(start).limit(crcPosition);
            CRC32C crc32c = new CRC32C();
            crc32c.update(protectedBytes);
            if ((int) crc32c.getValue() != expected) {
                throw corrupt(field + " CRC32C mismatch");
            }
        }

        private int consumed() {
            return source.position() - start;
        }

        private int remaining() {
            return source.remaining();
        }

        private void require(int length, String field) {
            if (length < 0 || source.remaining() < length) {
                throw corrupt(context + " is truncated at " + field);
            }
        }
    }

    @FunctionalInterface
    private interface IoAction {
        void run() throws IOException;
    }
}
