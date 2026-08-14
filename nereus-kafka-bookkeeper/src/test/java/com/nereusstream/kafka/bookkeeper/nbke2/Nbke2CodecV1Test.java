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

package com.nereusstream.kafka.bookkeeper.nbke2;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class Nbke2CodecV1Test {
    @Test
    void roundTripsEveryClosedFrameType() {
        List<Nbke2FrameV1> frames = List.of(
                Nbke2TestFrames.runHeader(),
                Nbke2TestFrames.data(),
                Nbke2TestFrames.rangeIndexBlock(),
                Nbke2TestFrames.protocolCheckpoint(),
                Nbke2TestFrames.runFooter());
        long[] entryIds = {0, 1, 3, 4, 5};

        for (int index = 0; index < frames.size(); index++) {
            long entryId = entryIds[index];
            byte[] encoded = Nbke2CodecV1.encode(Nbke2TestFrames.LEDGER_ID, entryId, frames.get(index));
            assertThat(Nbke2CodecV1.decode(encoded, Nbke2TestFrames.LEDGER_ID, entryId))
                    .isEqualTo(frames.get(index));
        }
    }

    @Test
    void commonHeaderIsCanonicalBigEndianAndLengthBound() {
        byte[] encoded = Nbke2CodecV1.encode(Nbke2TestFrames.LEDGER_ID, 0, Nbke2TestFrames.runHeader());
        ByteBuffer header = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);

        byte[] magic = new byte[Nbke2ConstantsV1.MAGIC.length];
        header.get(magic);
        assertThat(magic).containsExactly(Nbke2ConstantsV1.MAGIC);
        assertThat(Byte.toUnsignedInt(header.get())).isEqualTo(1);
        assertThat(Byte.toUnsignedInt(header.get())).isZero();
        assertThat(Byte.toUnsignedInt(header.get())).isEqualTo(Nbke2FrameTypeV1.RUN_HEADER.code());
        assertThat(Byte.toUnsignedInt(header.get())).isZero();
        assertThat(Byte.toUnsignedInt(header.get())).isZero();
        assertThat(Short.toUnsignedInt(header.getShort())).isEqualTo(Nbke2ConstantsV1.FIXED_HEADER_BYTES);
        assertThat(header.getInt()).isEqualTo(encoded.length);
        assertThat(header.getLong()).isEqualTo(Nbke2TestFrames.LEDGER_ID);
        assertThat(header.getLong()).isZero();
    }

    @Test
    void terminalDescriptorLivesOnlyInTheLastDataMember() {
        CanonicalBytes firstRaw = CanonicalBytes.copyOf(new byte[] {1});
        Nbke2DataV1 first = new Nbke2DataV1(
                Nbke2TestFrames.binding(), 100, 0, 0, 2, new Id128(0, 8), new Id128(0, 9), Optional.empty(), firstRaw);
        CanonicalBytes lastRaw = CanonicalBytes.copyOf(new byte[] {2});
        CanonicalBytes aggregate = CanonicalBytes.copyOf(new byte[] {1, 2});
        Nbke2DataV1 last = new Nbke2DataV1(
                Nbke2TestFrames.binding(),
                101,
                0,
                1,
                2,
                new Id128(0, 8),
                new Id128(0, 9),
                Optional.of(new Nbke2AppendGroupDescriptorV1(100, 102, 1, 2, Sha256Digest.hash(aggregate))),
                lastRaw);

        byte[] firstBytes = Nbke2CodecV1.encode(Nbke2TestFrames.LEDGER_ID, 1, first);
        byte[] lastBytes = Nbke2CodecV1.encode(Nbke2TestFrames.LEDGER_ID, 2, last);
        assertThat(Byte.toUnsignedInt(firstBytes[8])).isZero();
        assertThat(Byte.toUnsignedInt(lastBytes[8])).isEqualTo(Nbke2ConstantsV1.DATA_TERMINAL_DESCRIPTOR_FLAG);
        assertThat(Nbke2CodecV1.decode(firstBytes, Nbke2TestFrames.LEDGER_ID, 1))
                .isEqualTo(first);
        assertThat(Nbke2CodecV1.decode(lastBytes, Nbke2TestFrames.LEDGER_ID, 2)).isEqualTo(last);
    }

    @Test
    void maximumLegalDataFrameRoundTripsWithoutPersistedMaximumAllocation() {
        byte[] payload = new byte[Nbke2ConstantsV1.FORMAT_MAX_DATA_PAYLOAD_BYTES];
        payload[0] = 1;
        payload[payload.length - 1] = 2;
        CanonicalBytes raw = CanonicalBytes.copyOf(payload);
        Nbke2DataV1 maximum = new Nbke2DataV1(
                Nbke2TestFrames.binding(),
                0,
                0,
                0,
                1,
                new Id128(0, 1),
                new Id128(0, 2),
                Optional.of(new Nbke2AppendGroupDescriptorV1(0, 1, 1, 1, Sha256Digest.hash(raw))),
                raw);

        byte[] encoded = Nbke2CodecV1.encode(Nbke2TestFrames.LEDGER_ID, 1, maximum);
        assertThat(encoded.length).isLessThanOrEqualTo(Nbke2ConstantsV1.FORMAT_MAX_FRAME_BYTES);
        assertThat(Nbke2CodecV1.decode(encoded, Nbke2TestFrames.LEDGER_ID, 1)).isEqualTo(maximum);
    }
}
