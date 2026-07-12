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

package com.nereusstream.managedledger.entry;

import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendEntry;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadResult;
import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;

/** Byte-for-byte codec between one Pulsar managed-ledger Entry and one L0 offset. */
public final class PulsarEntryCodec {
    public static final String NUMBER_OF_MESSAGES_ATTRIBUTE = "pulsar.numberOfMessages";
    public static final String ENTRY_FORMAT_VERSION_ATTRIBUTE = "pulsar.entryFormatVersion";
    public static final String ENTRY_FORMAT_VERSION = "1";

    private final int maxEntryBytes;

    public PulsarEntryCodec(int maxEntryBytes) {
        if (maxEntryBytes <= 0) {
            throw new IllegalArgumentException("maxEntryBytes must be positive");
        }
        this.maxEntryBytes = maxEntryBytes;
    }

    public EncodedAppend encode(ByteBuf source, int numberOfMessages) {
        Objects.requireNonNull(source, "source");
        if (numberOfMessages < 1) {
            throw new IllegalArgumentException("numberOfMessages must be positive");
        }
        int readableBytes = source.readableBytes();
        if (readableBytes > maxEntryBytes) {
            throw new IllegalArgumentException("entry exceeds maxEntryBytes");
        }
        byte[] payload = new byte[readableBytes];
        source.getBytes(source.readerIndex(), payload);
        AppendEntry entry = new AppendEntry(
                payload,
                1,
                0,
                Map.of(
                        NUMBER_OF_MESSAGES_ATTRIBUTE,
                        Integer.toString(numberOfMessages),
                        ENTRY_FORMAT_VERSION_ATTRIBUTE,
                        ENTRY_FORMAT_VERSION));
        AppendBatch batch = new AppendBatch(
                PayloadFormat.OPAQUE_RECORD_BATCH,
                List.of(entry),
                1,
                1,
                0,
                0,
                List.of(),
                Map.of(),
                Optional.of(crc32c(payload)));
        return new EncodedAppend(batch, payload, numberOfMessages);
    }

    public Entry decode(Position position, ReadResult result) {
        Objects.requireNonNull(position, "position");
        Objects.requireNonNull(result, "result");
        long entryId = position.getEntryId();
        if (entryId < 0) {
            throw new IllegalArgumentException("read entry position must be non-negative");
        }
        long nextOffset;
        try {
            nextOffset = Math.addExact(entryId, 1);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("read entry position overflows", e);
        }
        if (result.requestedOffset() != entryId
                || result.nextOffset() != nextOffset
                || result.batches().size() != 1) {
            throw new IllegalArgumentException("read result does not describe exactly the requested entry");
        }
        ReadBatch batch = result.batches().get(0);
        if (!batch.range().equals(new OffsetRange(entryId, nextOffset))
                || batch.payloadFormat() != PayloadFormat.OPAQUE_RECORD_BATCH
                || !batch.schemaRefs().isEmpty()
                || batch.projectionRef().isPresent()) {
            throw new IllegalArgumentException("read batch violates PULSAR_ENTRY_V1");
        }
        byte[] payload = batch.payload();
        if (payload.length > maxEntryBytes) {
            throw new IllegalArgumentException("read entry exceeds maxEntryBytes");
        }
        return new NereusEntry(position, payload);
    }

    private static Checksum crc32c(byte[] payload) {
        CRC32C crc32c = new CRC32C();
        crc32c.update(payload, 0, payload.length);
        return new Checksum(
                ChecksumType.CRC32C,
                String.format(Locale.ROOT, "%08x", crc32c.getValue()));
    }
}
