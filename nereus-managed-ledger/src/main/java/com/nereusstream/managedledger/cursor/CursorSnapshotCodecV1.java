/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.api.Checksum;
import com.nereusstream.objectstore.Crc32cChecksums;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.zip.CRC32C;

/** Strict NCS1 full acknowledgement-state snapshot codec. */
public final class CursorSnapshotCodecV1 {
    private static final byte[] HEADER_MAGIC = ascii("NCS1");
    private static final byte[] RANGE_MAGIC = ascii("NCR1");
    private static final byte[] PARTIAL_MAGIC = ascii("NCB1");
    private static final byte[] FOOTER_MAGIC = ascii("NCF1");
    private static final int MAJOR_VERSION = 1;
    private static final int MINOR_VERSION = 0;
    private static final int OBJECT_TYPE_CODE = 4;
    private static final int FLAGS = 0;
    private static final int FOOTER_LENGTH = 4 + Short.BYTES * 2 + Long.BYTES * 3 + Integer.BYTES;

    private CursorSnapshotCodecV1() {
    }

    public static EncodedSnapshot encode(
            CursorSnapshotWriteRequest request,
            String snapshotId,
            CursorStorageConfig config) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(config, "config");
        byte[] snapshotIdBytes = decodeSnapshotId(snapshotId);
        CursorAckState state = request.fullState();
        List<OffsetRange> ranges = state.wholeAckRanges();
        NavigableMap<Long, BatchAckState> partials = state.partialBatchAcks();

        byte[] streamId = strictUtf8(request.identity().ledger().projection().streamId(), "streamId");
        byte[] managedLedgerHash = strictUtf8(
                request.identity().ledger().managedLedgerNameHash(), "managedLedgerNameHash");
        byte[] cursorHash = strictUtf8(request.identity().cursorNameHash(), "cursorNameHash");

        long headerLength = Math.addExact(
                4L + Short.BYTES * 4L + Integer.BYTES + Long.BYTES + 16L
                        + stringSize(streamId) + stringSize(managedLedgerHash) + stringSize(cursorHash)
                        + Long.BYTES * 7L + Integer.BYTES * 3L,
                0L);
        long rangeLength = Math.addExact(
                4L + Long.BYTES + Integer.BYTES,
                Math.multiplyExact((long) ranges.size(), Long.BYTES * 2L));
        long partialPayload = 0;
        for (Map.Entry<Long, BatchAckState> entry : partials.entrySet()) {
            partialPayload = Math.addExact(partialPayload,
                    Long.BYTES + Integer.BYTES * 2L
                            + Math.multiplyExact((long) entry.getValue().remainingWords().length, Long.BYTES));
        }
        long partialLength = Math.addExact(4L + Long.BYTES + Integer.BYTES, partialPayload);
        long totalLength = Math.addExact(
                Math.addExact(Math.addExact(headerLength, rangeLength), partialLength), FOOTER_LENGTH);
        if (totalLength <= 0 || totalLength > config.cursorSnapshotMaxBytes() || totalLength > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("cursor snapshot exceeds the configured object bound");
        }

        Writer header = new Writer();
        header.bytes(HEADER_MAGIC);
        header.u16(MAJOR_VERSION);
        header.u16(MINOR_VERSION);
        header.u16(OBJECT_TYPE_CODE);
        header.u16(FLAGS);
        header.u32(Math.toIntExact(headerLength));
        header.i64(totalLength);
        header.bytes(snapshotIdBytes);
        header.string(streamId);
        header.string(managedLedgerHash);
        header.string(cursorHash);
        header.i64(request.identity().ledger().projection().storageClassBindingGeneration());
        header.i64(request.identity().ledger().projection().incarnation());
        header.i64(request.identity().ledger().projection().virtualLedgerId());
        header.i64(request.identity().cursorGeneration());
        header.i64(request.sourceMutationSequence());
        header.i64(state.markDeleteOffset());
        header.i64(request.createdAtMillis());
        header.u32(ranges.size());
        header.u32(partials.size());
        header.i32(crc32c(header.toByteArray()));
        requireLength(header.size(), headerLength, "header");

        Writer rangeSection = new Writer();
        rangeSection.bytes(RANGE_MAGIC);
        rangeSection.i64(rangeLength);
        for (OffsetRange range : ranges) {
            rangeSection.i64(range.startOffset());
            rangeSection.i64(range.endOffset());
        }
        rangeSection.i32(crc32c(rangeSection.toByteArray()));
        requireLength(rangeSection.size(), rangeLength, "range section");

        Writer partialSection = new Writer();
        partialSection.bytes(PARTIAL_MAGIC);
        partialSection.i64(partialLength);
        for (Map.Entry<Long, BatchAckState> entry : partials.entrySet()) {
            partialSection.i64(entry.getKey());
            partialSection.i32(entry.getValue().batchSize());
            long[] words = entry.getValue().remainingWords();
            partialSection.u32(words.length);
            for (long word : words) {
                partialSection.i64(word);
            }
        }
        partialSection.i32(crc32c(partialSection.toByteArray()));
        requireLength(partialSection.size(), partialLength, "partial section");

        long minReferenced = state.markDeleteOffset();
        long maxReferenced = state.markDeleteOffset();
        for (OffsetRange range : ranges) {
            minReferenced = Math.min(minReferenced, range.startOffset());
            maxReferenced = Math.max(maxReferenced, range.endOffset());
        }
        for (long offset : partials.keySet()) {
            minReferenced = Math.min(minReferenced, offset);
            maxReferenced = Math.max(maxReferenced, Math.addExact(offset, 1));
        }

        Writer withoutFormatCrc = new Writer();
        withoutFormatCrc.bytes(header.toByteArray());
        withoutFormatCrc.bytes(rangeSection.toByteArray());
        withoutFormatCrc.bytes(partialSection.toByteArray());
        withoutFormatCrc.bytes(FOOTER_MAGIC);
        withoutFormatCrc.u16(MAJOR_VERSION);
        withoutFormatCrc.u16(FLAGS);
        withoutFormatCrc.i64(minReferenced);
        withoutFormatCrc.i64(maxReferenced);
        withoutFormatCrc.i64(totalLength);
        int formatCrc32c = crc32c(withoutFormatCrc.toByteArray());

        Writer object = new Writer();
        object.bytes(withoutFormatCrc.toByteArray());
        object.i32(formatCrc32c);
        requireLength(object.size(), totalLength, "snapshot object");
        byte[] bytes = object.toByteArray();
        Checksum storageChecksum = Crc32cChecksums.checksum(bytes);
        return new EncodedSnapshot(
                ByteBuffer.wrap(bytes).asReadOnlyBuffer(),
                formatCrc32c,
                storageChecksum,
                totalLength);
    }

    public static CursorAckState decode(
            ByteBuffer input,
            CursorSnapshotReference reference,
            CursorIdentity expectedIdentity,
            CursorStorageConfig config) {
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(reference, "reference");
        Objects.requireNonNull(expectedIdentity, "expectedIdentity");
        Objects.requireNonNull(config, "config");
        ByteBuffer source = input.asReadOnlyBuffer();
        if (source.remaining() <= 0
                || source.remaining() > config.cursorSnapshotMaxBytes()
                || source.remaining() != reference.objectLength()) {
            throw corruption("snapshot length does not match the reference or configured bound");
        }
        byte[] bytes = new byte[source.remaining()];
        source.get(bytes);
        if (!Crc32cChecksums.checksum(bytes).equals(reference.storageChecksum())) {
            throw corruption("snapshot full-object checksum mismatch");
        }
        try {
            Reader reader = new Reader(bytes);
            reader.magic(HEADER_MAGIC, "header");
            requireEqual(reader.u16("majorVersion"), MAJOR_VERSION, "majorVersion");
            requireEqual(reader.u16("minorVersion"), MINOR_VERSION, "minorVersion");
            requireEqual(reader.u16("objectTypeCode"), OBJECT_TYPE_CODE, "objectTypeCode");
            requireEqual(reader.u16("headerFlags"), FLAGS, "header flags");
            int headerLength = reader.u32("headerLengthBytes");
            long totalLength = reader.i64("totalObjectLengthBytes");
            requireEqual(totalLength, bytes.length, "total object length");
            String snapshotId = HexFormat.of().formatHex(reader.bytes(16, "snapshotId"));
            String streamId = reader.string("streamId");
            String managedLedgerHash = reader.string("managedLedgerNameHash");
            String cursorHash = reader.string("cursorNameHash");
            long bindingGeneration = reader.i64("storageClassBindingGeneration");
            long incarnation = reader.i64("incarnation");
            long virtualLedgerId = reader.i64("virtualLedgerId");
            long cursorGeneration = reader.i64("cursorGeneration");
            long sourceSequence = reader.i64("sourceMutationSequence");
            long baseMarkDelete = reader.i64("baseMarkDeleteOffset");
            long createdAt = reader.i64("createdAtMillis");
            int rangeCount = reader.u32("wholeRangeCount");
            int partialCount = reader.u32("partialEntryCount");
            int headerCrcOffset = reader.position();
            int headerCrc = reader.i32("headerCrc32c");
            requireEqual(reader.position(), headerLength, "header length");
            requireEqual(crc32c(bytes, 0, headerCrcOffset), headerCrc, "header CRC32C");

            requireEqual(snapshotId, reference.snapshotId(), "snapshot ID");
            requireEqual(streamId, expectedIdentity.ledger().projection().streamId(), "stream ID");
            requireEqual(managedLedgerHash, expectedIdentity.ledger().managedLedgerNameHash(),
                    "managed-ledger name hash");
            requireEqual(cursorHash, expectedIdentity.cursorNameHash(), "cursor name hash");
            requireEqual(bindingGeneration,
                    expectedIdentity.ledger().projection().storageClassBindingGeneration(), "binding generation");
            requireEqual(incarnation, expectedIdentity.ledger().projection().incarnation(), "incarnation");
            requireEqual(virtualLedgerId,
                    expectedIdentity.ledger().projection().virtualLedgerId(), "virtual ledger ID");
            requireEqual(cursorGeneration, expectedIdentity.cursorGeneration(), "cursor generation");
            requireEqual(cursorGeneration, reference.cursorGeneration(), "reference cursor generation");
            requireEqual(sourceSequence, reference.sourceMutationSequence(), "source mutation sequence");
            requireEqual(baseMarkDelete, reference.baseMarkDeleteOffset(), "base mark-delete offset");
            requireEqual(createdAt, reference.createdAtMillis(), "snapshot creation time");

            int rangeStart = reader.position();
            reader.magic(RANGE_MAGIC, "range section");
            long rangeLength = reader.i64("range section length");
            if (rangeCount > reader.remaining() / (Long.BYTES * 2)) {
                throw corruption("range count exceeds the remaining object bound");
            }
            List<OffsetRange> ranges = new ArrayList<>(rangeCount);
            for (int index = 0; index < rangeCount; index++) {
                ranges.add(new OffsetRange(reader.i64("range start"), reader.i64("range end")));
            }
            int rangeCrcOffset = reader.position();
            int rangeCrc = reader.i32("range section CRC32C");
            requireEqual(reader.position() - rangeStart, rangeLength, "range section length");
            requireEqual(crc32c(bytes, rangeStart, rangeCrcOffset - rangeStart), rangeCrc,
                    "range section CRC32C");

            int partialStart = reader.position();
            reader.magic(PARTIAL_MAGIC, "partial section");
            long partialLength = reader.i64("partial section length");
            if (partialCount > reader.remaining() / (Long.BYTES + Integer.BYTES * 2)) {
                throw corruption("partial count exceeds the remaining object bound");
            }
            TreeMap<Long, BatchAckState> partials = new TreeMap<>();
            for (int index = 0; index < partialCount; index++) {
                long offset = reader.i64("partial entryOffset");
                int batchSize = reader.i32("partial batchSize");
                int wordCount = reader.u32("partial wordCount");
                if (wordCount > reader.remaining() / Long.BYTES) {
                    throw corruption("partial word count exceeds the remaining object bound");
                }
                long[] words = new long[wordCount];
                for (int word = 0; word < wordCount; word++) {
                    words[word] = reader.i64("partial remainingWord");
                }
                BatchAckState state = new BatchAckState(batchSize, words);
                if (state.isWholeEntryAcknowledged() || state.isAllRemaining()
                        || partials.put(offset, state) != null) {
                    throw corruption("noncanonical or duplicate partial batch state");
                }
            }
            int partialCrcOffset = reader.position();
            int partialCrc = reader.i32("partial section CRC32C");
            requireEqual(reader.position() - partialStart, partialLength, "partial section length");
            requireEqual(crc32c(bytes, partialStart, partialCrcOffset - partialStart), partialCrc,
                    "partial section CRC32C");

            int footerStart = reader.position();
            reader.magic(FOOTER_MAGIC, "footer");
            requireEqual(reader.u16("footer majorVersion"), MAJOR_VERSION, "footer majorVersion");
            requireEqual(reader.u16("footer flags"), FLAGS, "footer flags");
            long minReferenced = reader.i64("minReferencedOffset");
            long maxReferenced = reader.i64("maxReferencedOffsetExclusive");
            requireEqual(reader.i64("footer totalObjectLengthBytes"), totalLength, "footer total length");
            int formatCrcOffset = reader.position();
            int formatCrc = reader.i32("formatCrc32c");
            reader.requireConsumed();
            requireEqual(formatCrc, reference.formatCrc32c(), "reference format CRC32C");
            requireEqual(crc32c(bytes, 0, formatCrcOffset), formatCrc, "format CRC32C");
            requireEqual(bytes.length - footerStart, FOOTER_LENGTH, "footer length");

            long calculatedMin = baseMarkDelete;
            long calculatedMax = baseMarkDelete;
            for (OffsetRange range : ranges) {
                calculatedMin = Math.min(calculatedMin, range.startOffset());
                calculatedMax = Math.max(calculatedMax, range.endOffset());
            }
            for (long offset : partials.keySet()) {
                calculatedMin = Math.min(calculatedMin, offset);
                calculatedMax = Math.max(calculatedMax, Math.addExact(offset, 1));
            }
            requireEqual(minReferenced, calculatedMin, "minimum referenced offset");
            requireEqual(maxReferenced, calculatedMax, "maximum referenced offset");

            CursorAckState state = new CursorAckState(baseMarkDelete, ranges, partials);
            if (state.markDeleteOffset() != baseMarkDelete
                    || !state.wholeAckRanges().equals(ranges)
                    || !state.partialBatchAcks().equals(partials)) {
                throw corruption("snapshot acknowledgement state is not canonical");
            }
            return state;
        } catch (CursorSnapshotCorruptionException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new CursorSnapshotCorruptionException("malformed NCS1 cursor snapshot", e);
        }
    }

    public record EncodedSnapshot(
            ByteBuffer payload,
            int formatCrc32c,
            Checksum storageChecksum,
            long objectLength) {
        public EncodedSnapshot {
            payload = Objects.requireNonNull(payload, "payload").asReadOnlyBuffer();
            Objects.requireNonNull(storageChecksum, "storageChecksum");
            if (payload.remaining() != objectLength || objectLength <= 0) {
                throw new IllegalArgumentException("encoded snapshot payload length mismatch");
            }
        }

        @Override
        public ByteBuffer payload() {
            return payload.asReadOnlyBuffer();
        }
    }

    public static final class CursorSnapshotCorruptionException extends IllegalArgumentException {
        public CursorSnapshotCorruptionException(String message) {
            super(message);
        }

        public CursorSnapshotCorruptionException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static byte[] decodeSnapshotId(String snapshotId) {
        try {
            byte[] bytes = HexFormat.of().parseHex(
                    com.nereusstream.metadata.oxia.CursorIds.requireRandomId(snapshotId, "snapshotId"));
            if (bytes.length != 16) {
                throw new IllegalArgumentException("snapshotId must decode to 16 bytes");
            }
            return bytes;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid snapshotId", e);
        }
    }

    private static long stringSize(byte[] bytes) {
        return Math.addExact(Integer.BYTES, bytes.length);
    }

    private static void requireLength(long actual, long expected, String field) {
        if (actual != expected) {
            throw new IllegalStateException(field + " length mismatch: expected " + expected + " but got " + actual);
        }
    }

    private static void requireEqual(long actual, long expected, String field) {
        if (actual != expected) {
            throw corruption(field + " mismatch");
        }
    }

    private static void requireEqual(String actual, String expected, String field) {
        if (!Objects.equals(actual, expected)) {
            throw corruption(field + " mismatch");
        }
    }

    private static CursorSnapshotCorruptionException corruption(String message) {
        return new CursorSnapshotCorruptionException(message);
    }

    private static int crc32c(byte[] bytes) {
        return crc32c(bytes, 0, bytes.length);
    }

    private static int crc32c(byte[] bytes, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, offset, length);
        return (int) crc.getValue();
    }

    private static byte[] strictUtf8(String value, String field) {
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(Objects.requireNonNull(value, field)));
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(field + " must be valid UTF-8", e);
        }
    }

    private static String strictUtf8(byte[] bytes, String field) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException e) {
            throw corruption("invalid UTF-8 in " + field);
        }
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static final class Writer {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        int size() {
            return out.size();
        }

        byte[] toByteArray() {
            return out.toByteArray();
        }

        void bytes(byte[] bytes) {
            out.writeBytes(bytes);
        }

        void u16(int value) {
            if (value < 0 || value > 0xffff) {
                throw new IllegalArgumentException("unsigned short value is out of range");
            }
            bytes(ByteBuffer.allocate(Short.BYTES).putShort((short) value).array());
        }

        void u32(int value) {
            if (value < 0) {
                throw new IllegalArgumentException("unsigned int value exceeds the Java allocation range");
            }
            i32(value);
        }

        void i32(int value) {
            bytes(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
        }

        void i64(long value) {
            bytes(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
        }

        void string(byte[] value) {
            u32(value.length);
            bytes(value);
        }
    }

    private static final class Reader {
        private final byte[] bytes;
        private final ByteBuffer buffer;

        Reader(byte[] bytes) {
            this.bytes = bytes;
            this.buffer = ByteBuffer.wrap(bytes);
        }

        int position() {
            return buffer.position();
        }

        int remaining() {
            return buffer.remaining();
        }

        int u16(String field) {
            require(Short.BYTES, field);
            return Short.toUnsignedInt(buffer.getShort());
        }

        int u32(String field) {
            int value = i32(field);
            if (value < 0) {
                throw corruption(field + " exceeds the supported allocation range");
            }
            return value;
        }

        int i32(String field) {
            require(Integer.BYTES, field);
            return buffer.getInt();
        }

        long i64(String field) {
            require(Long.BYTES, field);
            return buffer.getLong();
        }

        String string(String field) {
            return strictUtf8(bytes(u32(field + " length"), field), field);
        }

        byte[] bytes(int length, String field) {
            require(length, field);
            byte[] value = new byte[length];
            buffer.get(value);
            return value;
        }

        void magic(byte[] expected, String field) {
            if (!Arrays.equals(bytes(expected.length, field + " magic"), expected)) {
                throw corruption("invalid " + field + " magic");
            }
        }

        void requireConsumed() {
            if (buffer.hasRemaining()) {
                throw corruption("snapshot contains trailing bytes");
            }
        }

        private void require(int length, String field) {
            if (length < 0 || buffer.remaining() < length) {
                throw corruption("truncated snapshot field: " + field);
            }
        }
    }
}
