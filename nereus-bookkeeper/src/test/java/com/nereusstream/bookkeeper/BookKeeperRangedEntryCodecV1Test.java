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

package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendEntry;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BookKeeperRangedEntryCodecV1Test {
    @Test
    void stableGoldenRoundTripsExactPayloadAndRecordCount() {
        byte[] encoded = BookKeeperRangedEntryCodecV1.encode(new AppendEntry(
                "abc".getBytes(StandardCharsets.UTF_8),
                3,
                1,
                Map.of()));

        assertThat(HexFormat.of().formatHex(encoded))
                .isEqualTo("4e424b45310000000300000003364b3fb7616263");
        BookKeeperRangedEntryCodecV1.DecodedEntry decoded =
                BookKeeperRangedEntryCodecV1.decode(encoded);
        assertThat(decoded.recordCount()).isEqualTo(3);
        assertThat(decoded.payload()).isEqualTo("abc".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void malformedHeaderAndPayloadCorruptionFailClosed() {
        byte[] encoded = BookKeeperRangedEntryCodecV1.encode(new AppendEntry(
                "abc".getBytes(StandardCharsets.UTF_8),
                3,
                1,
                Map.of()));
        byte[] badMagic = encoded.clone();
        badMagic[0] ^= 1;
        byte[] badLength = encoded.clone();
        badLength[12] = 4;
        byte[] badPayload = encoded.clone();
        badPayload[badPayload.length - 1] ^= 1;

        assertCode(badMagic, ErrorCode.UNSUPPORTED_FORMAT);
        assertCode(badLength, ErrorCode.UNSUPPORTED_FORMAT);
        assertCode(badPayload, ErrorCode.PRIMARY_WAL_CHECKSUM_MISMATCH);
    }

    private static void assertCode(byte[] encoded, ErrorCode expected) {
        assertThatThrownBy(() -> BookKeeperRangedEntryCodecV1.decode(encoded))
                .isInstanceOfSatisfying(NereusException.class, failure ->
                        assertThat(failure.code()).isEqualTo(expected));
    }
}
