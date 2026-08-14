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

package com.nereusstream.storage.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.proto.OperationType;
import org.apache.bookkeeper.proto.ProtocolVersion;
import org.apache.bookkeeper.proto.Request;
import org.junit.jupiter.api.Test;

class BookKeeperV3Crc32cAddPayloadLimitV1Test {
    private static final int FRAME_LIMIT = 5 * 1024 * 1024;

    @Test
    void derivesTheExactRelease4180NormalAddPayloadAllowance() {
        int maximum = BookKeeperV3Crc32cAddPayloadLimitV1.maximumAddPayloadBytes(FRAME_LIMIT, FRAME_LIMIT);

        assertThat(maximum).isEqualTo(5_242_771);
        assertThat(BookKeeperV3Crc32cAddPayloadLimitV1.encodedWireFrameBytes(maximum))
                .isEqualTo(FRAME_LIMIT);
        assertThat(BookKeeperV3Crc32cAddPayloadLimitV1.encodedWireFrameBytes(maximum + 1))
                .isEqualTo(FRAME_LIMIT + 1);
    }

    @Test
    void formulaMatchesTheSourceLockedBookKeeperLightProtobufSerializer() {
        int maximum = BookKeeperV3Crc32cAddPayloadLimitV1.maximumAddPayloadBytes(FRAME_LIMIT, FRAME_LIMIT);

        assertThat(actualWireBytes(maximum))
                .isEqualTo(BookKeeperV3Crc32cAddPayloadLimitV1.encodedWireFrameBytes(maximum));
        assertThat(actualWireBytes(maximum + 1))
                .isEqualTo(BookKeeperV3Crc32cAddPayloadLimitV1.encodedWireFrameBytes(maximum + 1));
    }

    @Test
    void rejectsInvalidOrInsufficientFrameLimits() {
        assertThatThrownBy(() -> BookKeeperV3Crc32cAddPayloadLimitV1.maximumAddPayloadBytes(0, FRAME_LIMIT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BookKeeperV3Crc32cAddPayloadLimitV1.maximumAddPayloadBytes(1, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot carry one");
    }

    private static int actualWireBytes(int addPayloadBytes) {
        ByteBuf body = Unpooled.buffer(BookKeeperV3Crc32cAddPayloadLimitV1.ENTRY_METADATA_BYTES
                + BookKeeperV3Crc32cAddPayloadLimitV1.CRC32C_BYTES
                + addPayloadBytes);
        try {
            body.writerIndex(body.capacity());
            Request request = new Request();
            request.setHeader()
                    .setVersion(ProtocolVersion.VERSION_THREE)
                    .setOperation(OperationType.ADD_ENTRY)
                    .setTxnId(Long.MAX_VALUE);
            request.setAddRequest()
                    .setLedgerId(Long.MAX_VALUE)
                    .setEntryId(Long.MAX_VALUE)
                    .setMasterKey(new byte[BookKeeperV3Crc32cAddPayloadLimitV1.MASTER_KEY_BYTES])
                    .setBody(body);
            return BookKeeperV3Crc32cAddPayloadLimitV1.LENGTH_PREFIX_BYTES + request.getSerializedSize();
        } finally {
            body.release();
        }
    }
}
