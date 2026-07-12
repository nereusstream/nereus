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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendEntry;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadResult;
import com.nereusstream.api.StreamId;
import com.nereusstream.managedledger.projection.VirtualLedgerProjection;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.junit.jupiter.api.Test;

class PulsarEntryCodecTest {
    private static final long LEDGER_ID = VirtualLedgerProjection.MIN_VIRTUAL_LEDGER_ID;
    private static final StreamId STREAM_ID = new StreamId("stream");

    @Test
    void encodeCopiesOnlyTheReadableSliceWithoutMutatingCallerBuffer() {
        PulsarEntryCodec codec = new PulsarEntryCodec(16);
        ByteBuf source = Unpooled.wrappedBuffer(new byte[] {9, 1, 2, 3, 8});
        source.setIndex(1, 4);
        int readerIndex = source.readerIndex();
        int writerIndex = source.writerIndex();
        int referenceCount = source.refCnt();
        try {
            EncodedAppend encoded = codec.encode(source, 8);

            assertThat(source.readerIndex()).isEqualTo(readerIndex);
            assertThat(source.writerIndex()).isEqualTo(writerIndex);
            assertThat(source.refCnt()).isEqualTo(referenceCount);
            assertThat(encoded.callbackBytes()).containsExactly(1, 2, 3);
            assertThat(encoded.numberOfMessages()).isEqualTo(8);
            assertThat(encoded.appendBatch().recordCount()).isEqualTo(1);
            assertThat(encoded.appendBatch().entryCount()).isEqualTo(1);
            assertThat(encoded.appendBatch().entries().getFirst().payload()).containsExactly(1, 2, 3);
            assertThat(encoded.appendBatch().entries().getFirst().attributes())
                    .containsEntry(PulsarEntryCodec.NUMBER_OF_MESSAGES_ATTRIBUTE, "8")
                    .containsEntry(PulsarEntryCodec.ENTRY_FORMAT_VERSION_ATTRIBUTE, "1");
            Entry decoded = codec.decode(position(42), readResult(42, encoded.callbackBytes()));
            assertThat(decoded.getData()).containsExactly(1, 2, 3);
            decoded.release();

            byte[] returned = encoded.callbackBytes();
            returned[0] = 99;
            source.setByte(1, 77);
            assertThat(encoded.callbackBytes()).containsExactly(1, 2, 3);
            assertThat(encoded.appendBatch().entries().getFirst().payload()).containsExactly(1, 2, 3);
        } finally {
            source.release();
        }
    }

    @Test
    void crc32cUsesCanonicalLeadingZeroHex() {
        PulsarEntryCodec codec = new PulsarEntryCodec(1);
        ByteBuf source = Unpooled.wrappedBuffer(new byte[] {15});
        try {
            EncodedAppend encoded = codec.encode(source, 1);
            Checksum checksum = new Checksum(ChecksumType.CRC32C, "0c6e6f75");
            assertThat(encoded.appendBatch().checksum()).contains(checksum);
            assertThatThrownBy(() -> new AppendBatch(
                    PayloadFormat.OPAQUE_RECORD_BATCH,
                    List.of(new AppendEntry(new byte[] {14}, 1, 0, Map.of())),
                    1,
                    1,
                    0,
                    0,
                    List.of(),
                    Map.of(),
                    Optional.of(checksum)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("checksum mismatch");
        } finally {
            source.release();
        }
    }

    @Test
    void zeroByteEntryRoundTripsAndStillConsumesOneOffset() {
        PulsarEntryCodec codec = new PulsarEntryCodec(16);
        ByteBuf source = Unpooled.buffer(0, 0);
        try {
            EncodedAppend encoded = codec.encode(source, 1);
            Entry decoded = codec.decode(position(42), readResult(42, encoded.callbackBytes()));
            try {
                assertThat(decoded.getLength()).isZero();
                assertThat(decoded.getData()).isEmpty();
            } finally {
                decoded.release();
                assertThat(decoded.release()).isFalse();
            }
        } finally {
            source.release();
        }
    }

    @Test
    void decodeBuildsOneOwnedReadOnlyEntryAndReleaseIsIdempotent() {
        PulsarEntryCodec codec = new PulsarEntryCodec(16);
        Position position = position(42);
        Entry decoded = codec.decode(position, readResult(42, new byte[] {1, 2, 3}));

        assertThat(decoded).isInstanceOf(NereusEntry.class);
        assertThat(decoded.getPosition()).isEqualTo(position);
        assertThat(decoded.getLedgerId()).isEqualTo(LEDGER_ID);
        assertThat(decoded.getEntryId()).isEqualTo(42);
        assertThat(decoded.getData()).containsExactly(1, 2, 3);
        assertThat(decoded.getDataBuffer().isReadOnly()).isTrue();
        assertThat(decoded.getDataBuffer().readerIndex()).isZero();
        assertThat(decoded.getMessageMetadata()).isNull();
        assertThat(decoded.hasExpectedReads()).isFalse();
        assertThat(decoded.matchesPosition(position)).isTrue();

        assertThat(decoded.release()).isTrue();
        assertThat(decoded.release()).isFalse();
        assertThatThrownBy(decoded::getData).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(decoded::getDataBuffer).isInstanceOf(IllegalStateException.class);

        NereusEntry releasedByCopy = new NereusEntry(position(43), new byte[] {4, 5});
        assertThat(releasedByCopy.getDataAndRelease()).containsExactly(4, 5);
        assertThat(releasedByCopy.release()).isFalse();
    }

    @Test
    void lazilyParsesPulsarMetadataWithoutMovingOwnedIndices() {
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("producer")
                .setSequenceId(1)
                .setPublishTime(1234);
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[] {4, 5});
        ByteBuf serialized = Commands.serializeMetadataAndPayload(
                Commands.ChecksumType.Crc32c, metadata, payload);
        byte[] entryBytes = new byte[serialized.readableBytes()];
        serialized.getBytes(serialized.readerIndex(), entryBytes);
        serialized.release();
        payload.release();

        NereusEntry entry = new NereusEntry(position(7), entryBytes);
        try {
            int readerIndex = entry.getDataBuffer().readerIndex();
            assertThat(entry.getMessageMetadata()).isNotNull();
            assertThat(entry.getMessageMetadata().getPublishTime()).isEqualTo(1234);
            assertThat(entry.getEntryTimestamp()).isEqualTo(1234);
            assertThat(entry.getDataBuffer().readerIndex()).isEqualTo(readerIndex);
        } finally {
            entry.release();
        }
    }

    @Test
    void rejectsOversizedOrStructurallyInvalidEntries() {
        PulsarEntryCodec codec = new PulsarEntryCodec(2);
        ByteBuf oversized = Unpooled.wrappedBuffer(new byte[] {1, 2, 3});
        try {
            assertThatThrownBy(() -> codec.encode(oversized, 1))
                    .isInstanceOf(IllegalArgumentException.class);
        } finally {
            oversized.release();
        }

        assertThatThrownBy(() -> codec.decode(position(42), new ReadResult(
                STREAM_ID, 42, 44, List.of(readBatch(42, new byte[] {1})), false)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> codec.decode(PositionFactory.create(LEDGER_ID, -1), readResult(0, new byte[0])))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static Position position(long entryId) {
        return PositionFactory.create(LEDGER_ID, entryId);
    }

    private static ReadResult readResult(long offset, byte[] payload) {
        return new ReadResult(STREAM_ID, offset, offset + 1, List.of(readBatch(offset, payload)), false);
    }

    private static ReadBatch readBatch(long offset, byte[] payload) {
        return new ReadBatch(
                new OffsetRange(offset, offset + 1),
                PayloadFormat.OPAQUE_RECORD_BATCH,
                payload,
                List.of(),
                new EntryIndexRef(
                        EntryIndexLocation.OBJECT_FOOTER,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        0,
                        1,
                        checksum()),
                Optional.empty(),
                new ObjectId("object"),
                0,
                payload.length);
    }

    private static Checksum checksum() {
        return new Checksum(ChecksumType.CRC32C, "00000000");
    }
}
