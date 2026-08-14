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

package com.nereusstream.pulsar.offload.npd1;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdException;
import com.nereusstream.pulsar.offload.PulsarOffloadLimitCandidateV1;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

/** Canonical block-local NPD1/NPB1 encoder and targeted decoder. */
public final class Npd1CodecV1 {
    public static final int DATA_HEADER_BYTES = 32;
    public static final int BLOCK_HEADER_BYTES = 64;
    public static final int DIRECTORY_ROW_BYTES = 16;
    public static final int GCM_TAG_BYTES = 16;
    public static final int VERSION = 1;

    private static final int NPD1_MAGIC = 0x4e504431;
    private static final int NPB1_MAGIC = 0x4e504231;
    private static final int GCM_TAG_BITS = GCM_TAG_BYTES * 8;
    private static final byte[] NONCE_DOMAIN =
            "NPD1-NPB1-NONCE-V1".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

    private Npd1CodecV1() {}

    public enum CompressionFamily {
        NONE(0),
        ZSTD(1);

        private final int id;

        CompressionFamily(int id) {
            this.id = id;
        }

        int id() {
            return id;
        }

        static CompressionFamily fromId(int id) {
            return switch (id) {
                case 0 -> NONE;
                case 1 -> ZSTD;
                default -> throw rejected("unknown compression family");
            };
        }
    }

    public enum EncryptionFamily {
        AES_GCM_256(1);

        private final int id;

        EncryptionFamily(int id) {
            this.id = id;
        }

        int id() {
            return id;
        }

        static EncryptionFamily fromId(int id) {
            if (id != 1) {
                throw rejected("unknown encryption family");
            }
            return AES_GCM_256;
        }
    }

    public record EntryPayload(long entryId, byte[] payload) {
        public EntryPayload {
            Objects.requireNonNull(payload, "payload");
            payload = payload.clone();
            if (entryId < 0) {
                throw rejected("entry ID is negative");
            }
        }

        @Override
        public byte[] payload() {
            return payload.clone();
        }
    }

    public record SparseBlock(
            int blockOrdinal,
            long firstEntryId,
            int entryCount,
            long blockOffset,
            int encodedBlockBytes,
            long decodedBlockBytes,
            CompressionFamily compressionFamily,
            EncryptionFamily encryptionFamily,
            String encodedBlockSha256) {
        public SparseBlock {
            Objects.requireNonNull(compressionFamily, "compressionFamily");
            Objects.requireNonNull(encryptionFamily, "encryptionFamily");
            Objects.requireNonNull(encodedBlockSha256, "encodedBlockSha256");
            if (blockOrdinal < 0
                    || firstEntryId < 0
                    || entryCount <= 0
                    || blockOffset < DATA_HEADER_BYTES
                    || encodedBlockBytes <= BLOCK_HEADER_BYTES + GCM_TAG_BYTES
                    || decodedBlockBytes < 0
                    || !encodedBlockSha256.matches("[0-9a-f]{64}")) {
                throw rejected("sparse block fact is invalid");
            }
        }

        public long lastEntryId() {
            return Math.addExact(firstEntryId, entryCount - 1L);
        }
    }

    public record DataObject(
            Path path, long bytes, String sha256, long firstEntryId, long lastEntryId, List<SparseBlock> blocks) {
        public DataObject {
            Objects.requireNonNull(path, "path");
            Objects.requireNonNull(sha256, "sha256");
            blocks = List.copyOf(blocks);
            if (bytes <= DATA_HEADER_BYTES
                    || !sha256.matches("[0-9a-f]{64}")
                    || firstEntryId != 0
                    || lastEntryId < firstEntryId
                    || blocks.isEmpty()) {
                throw rejected("data Object descriptor is invalid");
            }
        }
    }

    public record DataHeader(int blockCount, long totalBytes) {
        public DataHeader {
            if (blockCount <= 0 || totalBytes <= DATA_HEADER_BYTES) {
                throw rejected("data header count or length is invalid");
            }
        }
    }

    public static DataObject encode(
            Path target,
            List<EntryPayload> entries,
            int blockTargetBytes,
            CompressionFamily compression,
            SecretKey attemptKey,
            UUID attemptUuid,
            PulsarOffloadLimitCandidateV1 limits) {
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(entries, "entries");
        Objects.requireNonNull(compression, "compression");
        validateKey(attemptKey);
        Objects.requireNonNull(attemptUuid, "attemptUuid");
        Objects.requireNonNull(limits, "limits");
        if (!limits.blockTargetBytes().contains(blockTargetBytes)) {
            throw rejected("block target is not one of the evidence candidates");
        }
        List<List<EntryPayload>> blocks = plan(entries, blockTargetBytes, limits);
        if (blocks.size() > 65_536) {
            throw rejected("NPD1 block count exceeds NPO1 sparse-row cap");
        }

        List<SparseBlock> sparse = new ArrayList<>(blocks.size());
        try {
            Path parent = target.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            try (RandomAccessFile output = new RandomAccessFile(target.toFile(), "rw")) {
                output.setLength(0);
                output.write(new byte[DATA_HEADER_BYTES]);
                long offset = DATA_HEADER_BYTES;
                for (int ordinal = 0; ordinal < blocks.size(); ordinal++) {
                    EncodedBlock encoded =
                            encodeBlock(blocks.get(ordinal), ordinal, compression, attemptKey, attemptUuid);
                    output.write(encoded.bytes());
                    sparse.add(new SparseBlock(
                            ordinal,
                            blocks.get(ordinal).get(0).entryId(),
                            blocks.get(ordinal).size(),
                            offset,
                            encoded.bytes().length,
                            encoded.decodedBytes(),
                            compression,
                            EncryptionFamily.AES_GCM_256,
                            sha256(encoded.bytes())));
                    offset = checkedAdd(offset, encoded.bytes().length);
                    if (offset > limits.maxDataObjectBytes()) {
                        throw rejected("encoded data Object crosses candidate maximum");
                    }
                }
                output.seek(0);
                output.write(dataHeader(sparse.size(), offset));
                output.setLength(offset);
            }
        } catch (IOException failure) {
            throw new Npd1RejectedException("cannot encode NPD1 target", failure);
        }
        long length;
        try {
            length = Files.size(target);
        } catch (IOException failure) {
            throw new Npd1RejectedException("cannot stat NPD1 target", failure);
        }
        long lastEntryId = entries.get(entries.size() - 1).entryId();
        return new DataObject(target, length, sha256(target), 0, lastEntryId, sparse);
    }

    public static DataHeader parseDataHeader(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length != DATA_HEADER_BYTES) {
            throw rejected("NPD1 header length differs");
        }
        ByteBuffer input = bigEndian(bytes);
        if (input.getInt() != NPD1_MAGIC
                || Short.toUnsignedInt(input.getShort()) != VERSION
                || Short.toUnsignedInt(input.getShort()) != VERSION
                || input.getInt() != 0
                || input.getInt() != DATA_HEADER_BYTES) {
            throw rejected("NPD1 fixed header differs");
        }
        int blockCount = input.getInt();
        if (input.getInt() != 0) {
            throw rejected("NPD1 reserved bits are non-zero");
        }
        long totalLength = input.getLong();
        return new DataHeader(blockCount, totalLength);
    }

    public static List<EntryPayload> decodeBlock(
            byte[] encoded,
            SparseBlock expected,
            SecretKey attemptKey,
            UUID attemptUuid,
            PulsarOffloadLimitCandidateV1 limits) {
        Objects.requireNonNull(encoded, "encoded");
        Objects.requireNonNull(expected, "expected");
        validateKey(attemptKey);
        Objects.requireNonNull(attemptUuid, "attemptUuid");
        Objects.requireNonNull(limits, "limits");
        if (encoded.length != expected.encodedBlockBytes() || !sha256(encoded).equals(expected.encodedBlockSha256())) {
            throw rejected("encoded block length or SHA-256 differs from sparse row");
        }
        if (encoded.length <= BLOCK_HEADER_BYTES + GCM_TAG_BYTES) {
            throw rejected("encoded block is shorter than its envelope");
        }
        byte[] headerBytes = Arrays.copyOf(encoded, BLOCK_HEADER_BYTES);
        BlockHeader header = parseBlockHeader(headerBytes);
        if (header.ordinal() != expected.blockOrdinal()
                || header.firstEntryId() != expected.firstEntryId()
                || header.entryCount() != expected.entryCount()
                || header.decodedBytes() != expected.decodedBlockBytes()
                || header.compression() != expected.compressionFamily()
                || header.encryption() != expected.encryptionFamily()
                || !Arrays.equals(header.nonce(), nonce(attemptUuid, expected.blockOrdinal()))
                || encoded.length
                        != checkedInt(
                                checkedAdd(BLOCK_HEADER_BYTES, checkedAdd(header.plaintextBytes(), GCM_TAG_BYTES)))) {
            throw rejected("NPB1 header differs from authenticated sparse facts");
        }
        if (header.entryCount() > limits.maxEntriesPerBlock()
                || header.decodedBytes() > limits.maxDecodedBlockBytes()) {
            throw rejected("NPB1 actual count or decoded bytes exceed candidate cap");
        }

        byte[] ciphertext = Arrays.copyOfRange(encoded, BLOCK_HEADER_BYTES, encoded.length);
        byte[] plaintext = crypt(Cipher.DECRYPT_MODE, attemptKey, header.nonce(), headerBytes, ciphertext);
        if (plaintext.length != header.plaintextBytes()) {
            throw rejected("NPB1 plaintext length differs");
        }
        ByteBuffer directory = bigEndian(Arrays.copyOf(plaintext, header.directoryBytes()));
        byte[] compressed = Arrays.copyOfRange(plaintext, header.directoryBytes(), plaintext.length);
        byte[] decoded = decompress(header.compression(), compressed, checkedInt(header.decodedBytes()));

        List<EntryPayload> result = new ArrayList<>(header.entryCount());
        long expectedOffset = 0;
        for (int ordinal = 0; ordinal < header.entryCount(); ordinal++) {
            long offset = directory.getLong();
            long payloadLength = Integer.toUnsignedLong(directory.getInt());
            int flags = directory.getInt();
            if (offset != expectedOffset || flags != 0) {
                throw rejected("NPB1 directory is non-contiguous or carries unknown flags");
            }
            long end = checkedAdd(offset, payloadLength);
            if (end > decoded.length) {
                throw rejected("NPB1 directory range exceeds decoded payload");
            }
            long entryId = checkedAdd(header.firstEntryId(), ordinal);
            result.add(new EntryPayload(entryId, Arrays.copyOfRange(decoded, checkedInt(offset), checkedInt(end))));
            expectedOffset = end;
        }
        if (directory.hasRemaining() || expectedOffset != decoded.length) {
            throw rejected("NPB1 directory has trailing rows or does not cover decoded payload");
        }
        return result;
    }

    private static List<List<EntryPayload>> plan(
            List<EntryPayload> entries, int targetBytes, PulsarOffloadLimitCandidateV1 limits) {
        if (entries.isEmpty() || entries.get(0).entryId() != 0) {
            throw rejected("NPD1 requires non-empty entry coverage beginning at zero");
        }
        List<List<EntryPayload>> result = new ArrayList<>();
        List<EntryPayload> current = new ArrayList<>();
        long currentBytes = 0;
        long expectedEntryId = 0;
        for (EntryPayload entry : entries) {
            if (entry.entryId() != expectedEntryId) {
                throw rejected("entry IDs are not ordered and gap-free");
            }
            expectedEntryId = checkedAdd(expectedEntryId, 1);
            int payloadBytes = entry.payload.length;
            if (payloadBytes > limits.maxEntryBytes()) {
                throw rejected("entry payload exceeds candidate hard maximum");
            }
            boolean wouldCrossTarget = !current.isEmpty() && checkedAdd(currentBytes, payloadBytes) > targetBytes;
            boolean wouldCrossCount = current.size() == limits.maxEntriesPerBlock();
            if (wouldCrossTarget || wouldCrossCount) {
                result.add(List.copyOf(current));
                current.clear();
                currentBytes = 0;
            }
            current.add(entry);
            currentBytes = checkedAdd(currentBytes, payloadBytes);
            if (currentBytes > limits.maxDecodedBlockBytes()) {
                throw rejected("decoded block exceeds candidate hard maximum");
            }
            if (payloadBytes > targetBytes) {
                result.add(List.copyOf(current));
                current.clear();
                currentBytes = 0;
            }
        }
        if (!current.isEmpty()) {
            result.add(List.copyOf(current));
        }
        return result;
    }

    private static EncodedBlock encodeBlock(
            List<EntryPayload> entries, int ordinal, CompressionFamily compression, SecretKey key, UUID attemptUuid) {
        long decodedLength = 0;
        for (EntryPayload entry : entries) {
            decodedLength = checkedAdd(decodedLength, entry.payload.length);
        }
        int decodedBytes = checkedInt(decodedLength);
        ByteBuffer decoded = ByteBuffer.allocate(decodedBytes);
        ByteBuffer directory = ByteBuffer.allocate(checkedInt(Math.multiplyExact(entries.size(), DIRECTORY_ROW_BYTES)))
                .order(ByteOrder.BIG_ENDIAN);
        long decodedOffset = 0;
        for (EntryPayload entry : entries) {
            directory.putLong(decodedOffset).putInt(entry.payload.length).putInt(0);
            decoded.put(entry.payload);
            decodedOffset = checkedAdd(decodedOffset, entry.payload.length);
        }
        byte[] compressed = compress(compression, decoded.array());
        int plaintextBytes = checkedInt(checkedAdd(directory.array().length, compressed.length));
        byte[] nonce = nonce(attemptUuid, ordinal);
        byte[] header = blockHeader(
                ordinal,
                entries.size(),
                compression,
                entries.get(0).entryId(),
                decodedBytes,
                directory.array().length,
                compressed.length,
                nonce);
        ByteBuffer plaintext = ByteBuffer.allocate(plaintextBytes);
        plaintext.put(directory.array()).put(compressed);
        byte[] ciphertext = crypt(Cipher.ENCRYPT_MODE, key, nonce, header, plaintext.array());
        ByteBuffer encoded = ByteBuffer.allocate(checkedInt(checkedAdd(header.length, ciphertext.length)));
        encoded.put(header).put(ciphertext);
        return new EncodedBlock(encoded.array(), decodedLength);
    }

    private static byte[] dataHeader(int blockCount, long totalBytes) {
        ByteBuffer header = ByteBuffer.allocate(DATA_HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
        header.putInt(NPD1_MAGIC)
                .putShort((short) VERSION)
                .putShort((short) VERSION)
                .putInt(0)
                .putInt(DATA_HEADER_BYTES)
                .putInt(blockCount)
                .putInt(0)
                .putLong(totalBytes);
        return header.array();
    }

    private static byte[] blockHeader(
            int ordinal,
            int entryCount,
            CompressionFamily compression,
            long firstEntryId,
            long decodedBytes,
            int directoryBytes,
            int compressedBytes,
            byte[] nonce) {
        ByteBuffer header = ByteBuffer.allocate(BLOCK_HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
        header.putInt(NPB1_MAGIC)
                .putShort((short) VERSION)
                .putShort((short) 0)
                .putInt(BLOCK_HEADER_BYTES)
                .putInt(ordinal)
                .putInt(entryCount)
                .putShort((short) compression.id())
                .putShort((short) EncryptionFamily.AES_GCM_256.id())
                .putLong(firstEntryId)
                .putLong(decodedBytes)
                .putInt(directoryBytes)
                .putInt(compressedBytes)
                .put(nonce)
                .putInt(0);
        return header.array();
    }

    private static BlockHeader parseBlockHeader(byte[] bytes) {
        if (bytes.length != BLOCK_HEADER_BYTES) {
            throw rejected("NPB1 header length differs");
        }
        ByteBuffer input = bigEndian(bytes);
        if (input.getInt() != NPB1_MAGIC
                || Short.toUnsignedInt(input.getShort()) != VERSION
                || Short.toUnsignedInt(input.getShort()) != 0
                || input.getInt() != BLOCK_HEADER_BYTES) {
            throw rejected("NPB1 fixed header differs");
        }
        int ordinal = input.getInt();
        int entryCount = input.getInt();
        CompressionFamily compression = CompressionFamily.fromId(Short.toUnsignedInt(input.getShort()));
        EncryptionFamily encryption = EncryptionFamily.fromId(Short.toUnsignedInt(input.getShort()));
        long firstEntryId = input.getLong();
        long decodedBytes = input.getLong();
        int directoryBytes = input.getInt();
        int compressedBytes = input.getInt();
        byte[] nonce = new byte[12];
        input.get(nonce);
        if (input.getInt() != 0
                || ordinal < 0
                || entryCount <= 0
                || firstEntryId < 0
                || decodedBytes < 0
                || directoryBytes != Math.multiplyExact(entryCount, DIRECTORY_ROW_BYTES)
                || compressedBytes < 0) {
            throw rejected("NPB1 count, length, or reserved field differs");
        }
        return new BlockHeader(
                ordinal,
                entryCount,
                compression,
                encryption,
                firstEntryId,
                decodedBytes,
                directoryBytes,
                compressedBytes,
                nonce);
    }

    private static byte[] compress(CompressionFamily family, byte[] decoded) {
        return switch (family) {
            case NONE -> decoded;
            case ZSTD -> Zstd.compress(decoded);
        };
    }

    private static byte[] decompress(CompressionFamily family, byte[] compressed, int decodedBytes) {
        if (family == CompressionFamily.NONE) {
            if (compressed.length != decodedBytes) {
                throw rejected("NONE codec length differs");
            }
            return compressed;
        }
        try {
            byte[] decoded = Zstd.decompress(compressed, decodedBytes);
            if (decoded.length != decodedBytes) {
                throw rejected("ZSTD decoded length differs");
            }
            return decoded;
        } catch (ZstdException failure) {
            throw new Npd1RejectedException("ZSTD block cannot be decoded", failure);
        }
    }

    private static byte[] nonce(UUID attemptUuid, int ordinal) {
        ByteBuffer input = ByteBuffer.allocate(20 + NONCE_DOMAIN.length).order(ByteOrder.BIG_ENDIAN);
        input.putLong(attemptUuid.getMostSignificantBits())
                .putLong(attemptUuid.getLeastSignificantBits())
                .putInt(ordinal)
                .put(NONCE_DOMAIN);
        return Arrays.copyOf(digest().digest(input.array()), 12);
    }

    private static byte[] crypt(int mode, SecretKey key, byte[] nonce, byte[] aad, byte[] body) {
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(mode, key, new GCMParameterSpec(GCM_TAG_BITS, nonce));
            cipher.updateAAD(aad);
            return cipher.doFinal(body);
        } catch (GeneralSecurityException failure) {
            throw new Npd1RejectedException("NPB1 AES-GCM operation failed", failure);
        }
    }

    private static void validateKey(SecretKey key) {
        Objects.requireNonNull(key, "attemptKey");
        byte[] encoded = key.getEncoded();
        if (!"AES".equals(key.getAlgorithm()) || encoded == null || encoded.length != 32) {
            throw rejected("attempt key must be one encodable AES-256 key");
        }
    }

    private static ByteBuffer bigEndian(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
    }

    private static long checkedAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException failure) {
            throw new Npd1RejectedException("checked length or entry-ID addition overflowed", failure);
        }
    }

    private static int checkedInt(long value) {
        if (value < 0 || value > Integer.MAX_VALUE) {
            throw rejected("actual block allocation exceeds the Java array domain");
        }
        return (int) value;
    }

    private static String sha256(byte[] bytes) {
        return HexFormat.of().formatHex(digest().digest(bytes));
    }

    private static String sha256(Path path) {
        MessageDigest digest = digest();
        try (InputStream input = new BufferedInputStream(
                Files.newInputStream(path, java.nio.file.StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS))) {
            byte[] buffer = new byte[64 * 1_024];
            int count;
            while ((count = input.read(buffer)) != -1) {
                digest.update(buffer, 0, count);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (IOException failure) {
            throw new Npd1RejectedException("cannot hash encoded NPD1 target", failure);
        }
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("JDK lacks SHA-256", failure);
        }
    }

    private static Npd1RejectedException rejected(String message) {
        return new Npd1RejectedException(message);
    }

    private record EncodedBlock(byte[] bytes, long decodedBytes) {}

    private record BlockHeader(
            int ordinal,
            int entryCount,
            CompressionFamily compression,
            EncryptionFamily encryption,
            long firstEntryId,
            long decodedBytes,
            int directoryBytes,
            int compressedBytes,
            byte[] nonce) {
        private int plaintextBytes() {
            return checkedInt(checkedAdd(directoryBytes, compressedBytes));
        }
    }

    public static final class Npd1RejectedException extends IllegalArgumentException {
        public Npd1RejectedException(String message) {
            super(message);
        }

        public Npd1RejectedException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
