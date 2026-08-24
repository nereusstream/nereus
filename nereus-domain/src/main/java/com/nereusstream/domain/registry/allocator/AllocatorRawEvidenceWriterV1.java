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
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Streaming NAEA1/NARE1 writer. It accepts only individual union events and never aggregate pass/rate inputs.
 * Request-keyed shards preserve every one-request endpoint order even when its async completion runs on another
 * thread; no global event order is claimed.
 */
public final class AllocatorRawEvidenceWriterV1 implements AutoCloseable {
    public static final int PAYLOAD_HEADER_BYTES = 32;
    private static final int MAX_JUNIT_XML_BYTES = 16 * 1024 * 1024;
    private static final byte[] PAYLOAD_MAGIC = "NARE".getBytes(StandardCharsets.US_ASCII);
    private static final int PAYLOAD_SCHEMA = 1;
    private static final long EVENT_COUNT_OFFSET = AllocatorEvidenceAttachmentV1.HEADER_BYTES + 8L;
    private static final int SEGMENT_BYTES = 1024 * 1024;
    private static final int SEGMENT_SHARDS = 64;

    private final FileChannel channel;
    private final AllocatorEvidenceAttachmentKindV1 kind;
    private final AllocatorEvidenceSourceTupleV1 sourceTuple;
    private final AtomicLong eventCount = new AtomicLong();
    private final Segment[] segments = new Segment[SEGMENT_SHARDS];
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
    private volatile boolean closed;

    private AllocatorRawEvidenceWriterV1(
            Path target, AllocatorEvidenceAttachmentKindV1 kind, AllocatorEvidenceSourceTupleV1 sourceTuple) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.sourceTuple = Objects.requireNonNull(sourceTuple, "sourceTuple");
        if (kind == AllocatorEvidenceAttachmentKindV1.TEST) {
            throw new IllegalArgumentException("TEST attachments use writeJUnitReport");
        }
        Arrays.setAll(segments, ignored -> new Segment());
        Path exactTarget = requireExactTarget(target, kind);
        try {
            channel = FileChannel.open(
                    exactTarget, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ);
            ByteBuffer envelope = ByteBuffer.allocate(AllocatorEvidenceAttachmentV1.HEADER_BYTES);
            AllocatorEvidenceAttachmentV1.writeHeader(
                    envelope, kind, sourceTuple, nonZeroPlaceholder(), PAYLOAD_HEADER_BYTES);
            envelope.flip();
            writeFully(channel, envelope);
            ByteBuffer payloadHeader = ByteBuffer.allocate(PAYLOAD_HEADER_BYTES);
            payloadHeader
                    .put(PAYLOAD_MAGIC)
                    .putShort((short) PAYLOAD_SCHEMA)
                    .putShort((short) kind.code())
                    .putLong(0)
                    .put(new byte[16])
                    .flip();
            writeFully(channel, payloadHeader);
        } catch (IOException error) {
            throw new AllocatorProtocolException(
                    AllocatorProtocolException.Code.SELECTION_NOT_ELIGIBLE,
                    "allocator raw evidence file could not be created",
                    error);
        }
    }

    public static AllocatorRawEvidenceWriterV1 open(
            Path target, AllocatorEvidenceAttachmentKindV1 kind, AllocatorEvidenceSourceTupleV1 sourceTuple) {
        return new AllocatorRawEvidenceWriterV1(target, kind, sourceTuple);
    }

    public void append(AllocatorRawEvidenceEventV1 event) {
        Objects.requireNonNull(event, "event");
        lifecycleLock.readLock().lock();
        try {
            if (closed) {
                throw new IllegalStateException("allocator raw evidence writer is closed");
            }
            requireEventBelongsToAttachment(event);
            Segment segment = segments[segmentIndex(event)];
            synchronized (segment) {
                if (segment.buffer.remaining() < AllocatorRawEvidenceEventV1.BYTES) {
                    flushSegment(segment);
                }
                segment.buffer.put(event.encode().toByteArray());
            }
            long count = eventCount.incrementAndGet();
            if (PAYLOAD_HEADER_BYTES + Math.multiplyExact(count, AllocatorRawEvidenceEventV1.BYTES)
                    > AllocatorEvidenceAttachmentV1.MAX_PAYLOAD_BYTES) {
                throw AllocatorSelectionReceiptV1.invalid("allocator raw evidence payload exceeds 8 GiB");
            }
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    @Override
    public void close() {
        lifecycleLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            long exactEventCount = eventCount.get();
            if (exactEventCount == 0) {
                throw AllocatorSelectionReceiptV1.invalid("allocator raw evidence attachment has zero events");
            }
            for (Segment segment : segments) {
                flushSegment(segment);
            }
            ByteBuffer count = ByteBuffer.allocate(Long.BYTES).putLong(exactEventCount);
            count.flip();
            channel.position(EVENT_COUNT_OFFSET);
            writeFully(channel, count);
            long payloadLength = PAYLOAD_HEADER_BYTES + exactEventCount * AllocatorRawEvidenceEventV1.BYTES;
            Sha256Digest payloadDigest = digestPayload(channel, payloadLength);
            ByteBuffer header = ByteBuffer.allocate(AllocatorEvidenceAttachmentV1.HEADER_BYTES);
            AllocatorEvidenceAttachmentV1.writeHeader(header, kind, sourceTuple, payloadDigest, payloadLength);
            header.flip();
            channel.position(0);
            writeFully(channel, header);
            channel.truncate(AllocatorEvidenceAttachmentV1.HEADER_BYTES + payloadLength);
            channel.force(true);
        } catch (IOException error) {
            throw new AllocatorProtocolException(
                    AllocatorProtocolException.Code.SELECTION_NOT_ELIGIBLE,
                    "allocator raw evidence attachment could not be sealed",
                    error);
        } finally {
            closed = true;
            try {
                channel.close();
            } catch (IOException error) {
                // A close error leaves a file that the canonical parser must reject.
            }
            lifecycleLock.writeLock().unlock();
        }
    }

    private void flushSegment(Segment segment) {
        segment.buffer.flip();
        if (!segment.buffer.hasRemaining()) {
            segment.buffer.clear();
            return;
        }
        synchronized (channel) {
            try {
                writeFully(channel, segment.buffer);
            } catch (IOException error) {
                throw new AllocatorProtocolException(
                        AllocatorProtocolException.Code.SELECTION_NOT_ELIGIBLE,
                        "allocator raw evidence segment could not be written",
                        error);
            }
        }
        segment.buffer.clear();
    }

    public static void writeJUnitReport(Path target, AllocatorEvidenceSourceTupleV1 sourceTuple, Path exactJUnitXml) {
        Objects.requireNonNull(exactJUnitXml, "exactJUnitXml");
        Path exactTarget = requireExactTarget(target, AllocatorEvidenceAttachmentKindV1.TEST);
        byte[] xml;
        try (InputStream input = Files.newInputStream(exactJUnitXml, LinkOption.NOFOLLOW_LINKS)) {
            if (!Files.isRegularFile(exactJUnitXml, LinkOption.NOFOLLOW_LINKS)) {
                throw new IllegalArgumentException("JUnit XML must be one exact non-symlink regular file");
            }
            xml = input.readNBytes(MAX_JUNIT_XML_BYTES + 1);
        } catch (IOException error) {
            throw new AllocatorProtocolException(
                    AllocatorProtocolException.Code.SELECTION_NOT_ELIGIBLE,
                    "allocator JUnit XML could not be read",
                    error);
        }
        if (xml.length == 0 || xml.length > MAX_JUNIT_XML_BYTES) {
            throw new IllegalArgumentException("JUnit XML byte length is outside [1,16 MiB]");
        }
        AllocatorJUnitEvidenceV1.Counts counts = AllocatorJUnitEvidenceV1.parse(new java.io.ByteArrayInputStream(xml));
        ByteBuffer payload = ByteBuffer.allocate(64 + xml.length);
        payload.put("NAJT".getBytes(StandardCharsets.US_ASCII))
                .putShort((short) 1)
                .putShort((short) 0)
                .putLong(counts.tests())
                .putLong(counts.failures())
                .putLong(counts.errors())
                .putLong(counts.skipped())
                .putInt(xml.length)
                .put(new byte[20])
                .put(xml);
        CanonicalBytes envelope = AllocatorEvidenceAttachmentV1.canonicalForTest(
                AllocatorEvidenceAttachmentKindV1.TEST, sourceTuple, CanonicalBytes.copyOf(payload.array()));
        try (FileChannel output =
                FileChannel.open(exactTarget, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            writeFully(output, ByteBuffer.wrap(envelope.toByteArray()));
            output.force(true);
        } catch (IOException error) {
            throw new AllocatorProtocolException(
                    AllocatorProtocolException.Code.SELECTION_NOT_ELIGIBLE,
                    "allocator JUnit report attachment could not be written",
                    error);
        }
    }

    private void requireEventBelongsToAttachment(AllocatorRawEvidenceEventV1 event) {
        boolean valid =
                switch (kind) {
                    case NATIVE -> event.context().nativePath();
                    case SCALE_10K ->
                        !event.context().nativePath() && event.context().activeManagedLedgers() == 10_000;
                    case SCALE_100K ->
                        !event.context().nativePath() && event.context().activeManagedLedgers() == 100_000;
                    case FAULT -> !event.context().nativePath();
                    case TEST -> false;
                };
        if (!valid) {
            throw AllocatorSelectionReceiptV1.invalid("allocator raw event context belongs to another attachment");
        }
    }

    private static int segmentIndex(AllocatorRawEvidenceEventV1 event) {
        int requestHash = Long.hashCode(event.requestOrdinal());
        return (31 * event.context().contextId() + requestHash) & (SEGMENT_SHARDS - 1);
    }

    private static Sha256Digest digestPayload(FileChannel channel, long payloadLength) throws IOException {
        MessageDigest digest = sha256();
        channel.position(AllocatorEvidenceAttachmentV1.HEADER_BYTES);
        ByteBuffer buffer = ByteBuffer.allocate(64 * 1024);
        long remaining = payloadLength;
        while (remaining > 0) {
            buffer.clear().limit((int) Math.min(buffer.capacity(), remaining));
            int read = channel.read(buffer);
            if (read < 0) {
                throw new IOException("unexpected EOF while sealing allocator raw evidence");
            }
            digest.update(buffer.array(), 0, read);
            remaining -= read;
        }
        return Sha256Digest.copyOf(digest.digest());
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("JDK has no SHA-256 provider", error);
        }
    }

    private static Sha256Digest nonZeroPlaceholder() {
        return Sha256Digest.hash(CanonicalBytes.copyOf("NARE1-PENDING".getBytes(StandardCharsets.US_ASCII)));
    }

    private static Path requireExactTarget(Path target, AllocatorEvidenceAttachmentKindV1 kind) {
        Path exactTarget =
                Objects.requireNonNull(target, "target").toAbsolutePath().normalize();
        if (exactTarget.getFileName() == null
                || !exactTarget.getFileName().toString().equals(kind.fileName())) {
            throw AllocatorSelectionReceiptV1.invalid(
                    "allocator raw evidence writer target differs from its closed kind basename");
        }
        return exactTarget;
    }

    private static void writeFully(FileChannel channel, ByteBuffer source) throws IOException {
        while (source.hasRemaining()) {
            channel.write(source);
        }
    }

    private static final class Segment {
        private final ByteBuffer buffer = ByteBuffer.allocateDirect(SEGMENT_BYTES);
    }
}
