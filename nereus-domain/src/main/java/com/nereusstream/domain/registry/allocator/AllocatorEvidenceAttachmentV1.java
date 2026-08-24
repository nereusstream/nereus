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

package com.nereusstream.domain.registry.allocator;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;

/** Strict NAEA1 envelope binding a raw payload to exact sources, executor, and selected code artifact. */
public final class AllocatorEvidenceAttachmentV1 {
    public static final int HEADER_BYTES = 360;
    /** One population can exceed 2 GiB at 64 bytes/event; the closed five-file inventory admits up to 8 GiB each. */
    public static final long MAX_PAYLOAD_BYTES = 8L << 30;

    private static final byte[] MAGIC = "NAEA".getBytes(StandardCharsets.US_ASCII);
    private static final int SCHEMA_VERSION = 1;

    private final AllocatorEvidenceAttachmentKindV1 kind;
    private final AllocatorEvidenceSourceTupleV1 sourceTuple;
    private final Sha256Digest payloadSha256;
    private final long payloadLength;
    private final Sha256Digest envelopeSha256;
    private final CanonicalBytes inMemoryPayload;
    private final Path file;

    private AllocatorEvidenceAttachmentV1(
            AllocatorEvidenceAttachmentKindV1 kind,
            AllocatorEvidenceSourceTupleV1 sourceTuple,
            Sha256Digest payloadSha256,
            long payloadLength,
            Sha256Digest envelopeSha256,
            CanonicalBytes inMemoryPayload,
            Path file) {
        this.kind = kind;
        this.sourceTuple = sourceTuple;
        this.payloadSha256 = payloadSha256;
        this.payloadLength = payloadLength;
        this.envelopeSha256 = envelopeSha256;
        this.inMemoryPayload = inMemoryPayload;
        this.file = file;
    }

    public static AllocatorEvidenceAttachmentV1 parseCanonical(CanonicalBytes canonicalEnvelope) {
        Objects.requireNonNull(canonicalEnvelope, "canonicalEnvelope");
        if (canonicalEnvelope.length() < HEADER_BYTES) {
            throw AllocatorSelectionReceiptV1.invalid("allocator evidence attachment is truncated");
        }
        ByteBuffer input = ByteBuffer.wrap(canonicalEnvelope.toByteArray());
        ParsedHeader header = parseHeader(input);
        if (header.payloadLength() > Integer.MAX_VALUE || input.remaining() != header.payloadLength()) {
            throw AllocatorSelectionReceiptV1.invalid("allocator evidence attachment payload length differs");
        }
        byte[] payload = new byte[(int) header.payloadLength()];
        input.get(payload);
        CanonicalBytes canonicalPayload = CanonicalBytes.copyOf(payload);
        if (!Sha256Digest.hash(canonicalPayload).equals(header.payloadSha256())) {
            throw AllocatorSelectionReceiptV1.invalid("allocator evidence attachment payload digest differs");
        }
        return new AllocatorEvidenceAttachmentV1(
                header.kind(),
                header.sourceTuple(),
                header.payloadSha256(),
                header.payloadLength(),
                Sha256Digest.hash(canonicalEnvelope),
                canonicalPayload,
                null);
    }

    /** Streaming parser for formal files; neither the envelope nor a 100+ MiB event payload is copied into heap. */
    public static AllocatorEvidenceAttachmentV1 parseCanonical(Path canonicalEnvelopeFile) {
        Objects.requireNonNull(canonicalEnvelopeFile, "canonicalEnvelopeFile");
        if (!Files.isRegularFile(canonicalEnvelopeFile, LinkOption.NOFOLLOW_LINKS)) {
            throw AllocatorSelectionReceiptV1.invalid(
                    "allocator evidence attachment must be an exact non-symlink regular file");
        }
        try (FileChannel channel =
                FileChannel.open(canonicalEnvelopeFile, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            long fileLength = channel.size();
            if (fileLength < HEADER_BYTES || fileLength > HEADER_BYTES + MAX_PAYLOAD_BYTES) {
                throw AllocatorSelectionReceiptV1.invalid("allocator evidence attachment file length is invalid");
            }
            ByteBuffer headerBytes = ByteBuffer.allocate(HEADER_BYTES);
            readFully(channel, headerBytes);
            headerBytes.flip();
            ParsedHeader header = parseHeader(headerBytes);
            if (header.payloadLength() != fileLength - HEADER_BYTES) {
                throw AllocatorSelectionReceiptV1.invalid("allocator evidence attachment file payload length differs");
            }
            MessageDigest payloadDigest = sha256();
            MessageDigest envelopeDigest = sha256();
            envelopeDigest.update(headerBytes.array());
            channel.position(HEADER_BYTES);
            ByteBuffer buffer = ByteBuffer.allocate(64 * 1024);
            long remaining = header.payloadLength();
            while (remaining > 0) {
                buffer.clear().limit((int) Math.min(buffer.capacity(), remaining));
                int read = channel.read(buffer);
                if (read < 0) {
                    throw new IOException("unexpected EOF while hashing allocator evidence payload");
                }
                buffer.flip();
                envelopeDigest.update(buffer.asReadOnlyBuffer());
                payloadDigest.update(buffer);
                remaining -= read;
            }
            if (!MessageDigest.isEqual(
                    payloadDigest.digest(), header.payloadSha256().bytes().toByteArray())) {
                throw AllocatorSelectionReceiptV1.invalid("allocator evidence attachment file payload digest differs");
            }
            return new AllocatorEvidenceAttachmentV1(
                    header.kind(),
                    header.sourceTuple(),
                    header.payloadSha256(),
                    header.payloadLength(),
                    Sha256Digest.copyOf(envelopeDigest.digest()),
                    null,
                    canonicalEnvelopeFile.toAbsolutePath().normalize());
        } catch (IOException error) {
            throw new AllocatorProtocolException(
                    AllocatorProtocolException.Code.SELECTION_NOT_ELIGIBLE,
                    "allocator evidence attachment file could not be read",
                    error);
        }
    }

    public InputStream openPayload() {
        if (inMemoryPayload != null) {
            return new java.io.ByteArrayInputStream(inMemoryPayload.toByteArray());
        }
        try {
            FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
            channel.position(HEADER_BYTES);
            return new BoundedInputStream(
                    Channels.newInputStream(channel),
                    payloadLength,
                    payloadSha256.bytes().toByteArray());
        } catch (IOException error) {
            throw new AllocatorProtocolException(
                    AllocatorProtocolException.Code.SELECTION_NOT_ELIGIBLE,
                    "allocator evidence payload could not be opened",
                    error);
        }
    }

    static CanonicalBytes canonicalForTest(
            AllocatorEvidenceAttachmentKindV1 kind,
            AllocatorEvidenceSourceTupleV1 sourceTuple,
            CanonicalBytes payload) {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(sourceTuple, "sourceTuple");
        Objects.requireNonNull(payload, "payload");
        if (payload.isEmpty() || payload.length() > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("test attachment payload length is invalid");
        }
        ByteBuffer output = ByteBuffer.allocate(HEADER_BYTES + payload.length());
        writeHeader(output, kind, sourceTuple, Sha256Digest.hash(payload), payload.length());
        output.put(payload.toByteArray());
        return CanonicalBytes.copyOf(output.array());
    }

    static CanonicalBytes canonicalForTest(
            AllocatorEvidenceAttachmentKindV1 kind,
            String nereus,
            String pulsar,
            String oxia,
            Sha256Digest artifact,
            Sha256Digest locks,
            CanonicalBytes payload) {
        return canonicalForTest(
                kind,
                new AllocatorEvidenceSourceTupleV1(
                        nereus,
                        pulsar,
                        oxia,
                        oxia,
                        Sha256Digest.hash(CanonicalBytes.copyOf("oxia-jar".getBytes(StandardCharsets.US_ASCII))),
                        artifact,
                        artifact,
                        artifact,
                        artifact,
                        locks,
                        Sha256Digest.hash(CanonicalBytes.copyOf("executor".getBytes(StandardCharsets.US_ASCII)))),
                payload);
    }

    static void writeHeader(
            ByteBuffer output,
            AllocatorEvidenceAttachmentKindV1 kind,
            AllocatorEvidenceSourceTupleV1 sourceTuple,
            Sha256Digest payloadSha256,
            long payloadLength) {
        output.put(MAGIC).putShort((short) SCHEMA_VERSION).putShort((short) kind.code());
        putCommit(output, sourceTuple.nereusSourceCommit());
        putCommit(output, sourceTuple.pulsarSourceCommit());
        putCommit(output, sourceTuple.oxiaClientSourceCommit());
        putCommit(output, sourceTuple.oxiaServerSourceCommit());
        putDigest(output, sourceTuple.oxiaClientJarSha256());
        putDigest(output, sourceTuple.testedEvidenceArtifactSha256());
        putDigest(output, sourceTuple.runtimeDomainArtifactSha256());
        putDigest(output, sourceTuple.runtimeMetadataSpiArtifactSha256());
        putDigest(output, sourceTuple.runtimeMetadataOxiaArtifactSha256());
        putDigest(output, sourceTuple.sourceLocksSha256());
        putDigest(output, sourceTuple.executorManifestSha256());
        putDigest(output, payloadSha256);
        output.putLong(payloadLength).put(new byte[8]);
    }

    private static ParsedHeader parseHeader(ByteBuffer input) {
        byte[] magic = new byte[MAGIC.length];
        input.get(magic);
        if (!Arrays.equals(magic, MAGIC) || Short.toUnsignedInt(input.getShort()) != SCHEMA_VERSION) {
            throw AllocatorSelectionReceiptV1.invalid("allocator evidence attachment magic/schema differs");
        }
        AllocatorEvidenceAttachmentKindV1 kind =
                AllocatorEvidenceAttachmentKindV1.fromCode(Short.toUnsignedInt(input.getShort()));
        AllocatorEvidenceSourceTupleV1 tuple = new AllocatorEvidenceSourceTupleV1(
                readCommit(input),
                readCommit(input),
                readCommit(input),
                readCommit(input),
                readDigest(input),
                readDigest(input),
                readDigest(input),
                readDigest(input),
                readDigest(input),
                readDigest(input),
                readDigest(input));
        Sha256Digest payloadDigest = readDigest(input);
        long payloadLength = input.getLong();
        requireZero(input, 8);
        if (payloadLength <= 0 || payloadLength > MAX_PAYLOAD_BYTES || payloadDigest.isZero()) {
            throw AllocatorSelectionReceiptV1.invalid("allocator evidence attachment identity/length is invalid");
        }
        return new ParsedHeader(kind, tuple, payloadDigest, payloadLength);
    }

    public AllocatorEvidenceAttachmentKindV1 kind() {
        return kind;
    }

    public AllocatorEvidenceSourceTupleV1 sourceTuple() {
        return sourceTuple;
    }

    public String nereusSourceCommit() {
        return sourceTuple.nereusSourceCommit();
    }

    public String pulsarSourceCommit() {
        return sourceTuple.pulsarSourceCommit();
    }

    public String oxiaSourceCommit() {
        return sourceTuple.oxiaClientSourceCommit();
    }

    public Sha256Digest exactArtifactSha256() {
        return sourceTuple.runtimeDomainArtifactSha256();
    }

    public Sha256Digest sourceLocksSha256() {
        return sourceTuple.sourceLocksSha256();
    }

    public Sha256Digest payloadSha256() {
        return payloadSha256;
    }

    public long payloadLength() {
        return payloadLength;
    }

    public Sha256Digest envelopeSha256() {
        return envelopeSha256;
    }

    private static void putCommit(ByteBuffer output, String value) {
        output.put(HexFormat.of().parseHex(value));
    }

    static String readCommit(ByteBuffer input) {
        byte[] value = new byte[20];
        input.get(value);
        return HexFormat.of().formatHex(value);
    }

    static void putDigest(ByteBuffer output, Sha256Digest value) {
        output.put(value.bytes().toByteArray());
    }

    static Sha256Digest readDigest(ByteBuffer input) {
        byte[] value = new byte[Sha256Digest.LENGTH];
        input.get(value);
        return Sha256Digest.copyOf(value);
    }

    static void requireZero(ByteBuffer input, int length) {
        for (int index = 0; index < length; index++) {
            if (input.get() != 0) {
                throw AllocatorSelectionReceiptV1.invalid("allocator receipt reserved bytes must be zero");
            }
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("JDK has no SHA-256 provider", error);
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer target) throws IOException {
        while (target.hasRemaining()) {
            if (channel.read(target) < 0) {
                throw new IOException("unexpected EOF");
            }
        }
    }

    private record ParsedHeader(
            AllocatorEvidenceAttachmentKindV1 kind,
            AllocatorEvidenceSourceTupleV1 sourceTuple,
            Sha256Digest payloadSha256,
            long payloadLength) {}

    private static final class BoundedInputStream extends java.io.FilterInputStream {
        private long remaining;
        private final MessageDigest digest = sha256();
        private final byte[] expectedDigest;
        private boolean verified;

        private BoundedInputStream(InputStream input, long remaining, byte[] expectedDigest) {
            super(input);
            this.remaining = remaining;
            this.expectedDigest = expectedDigest.clone();
        }

        @Override
        public int read() throws IOException {
            if (remaining == 0) {
                return -1;
            }
            int value = super.read();
            if (value < 0) {
                throw new IOException("allocator evidence payload ended before its canonical length");
            }
            digest.update((byte) value);
            remaining--;
            verifyAtEnd();
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            if (remaining == 0) {
                return -1;
            }
            int read = super.read(bytes, offset, (int) Math.min(length, remaining));
            if (read < 0) {
                throw new IOException("allocator evidence payload ended before its canonical length");
            }
            if (read == 0) {
                return 0;
            }
            digest.update(bytes, offset, read);
            remaining -= read;
            verifyAtEnd();
            return read;
        }

        private void verifyAtEnd() throws IOException {
            if (remaining == 0 && !verified) {
                verified = true;
                if (!MessageDigest.isEqual(digest.digest(), expectedDigest)) {
                    throw new IOException("allocator evidence payload changed after envelope validation");
                }
            }
        }
    }
}
