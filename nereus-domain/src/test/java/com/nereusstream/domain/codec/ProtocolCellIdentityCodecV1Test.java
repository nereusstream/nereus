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

package com.nereusstream.domain.codec;

import static com.nereusstream.domain.DomainTestFixtures.KAFKA_NPC1_HEX;
import static com.nereusstream.domain.DomainTestFixtures.PULSAR_NPC1_HEX;
import static com.nereusstream.domain.DomainTestFixtures.hex;
import static com.nereusstream.domain.DomainTestFixtures.kafkaCell;
import static com.nereusstream.domain.DomainTestFixtures.pulsarCell;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class ProtocolCellIdentityCodecV1Test {
    @Test
    void encodesAndDecodesLiteralKafkaAndPulsarGoldens() {
        assertThat(ProtocolCellIdentityCodecV1.encode(kafkaCell()).toHex()).isEqualTo(KAFKA_NPC1_HEX);
        assertThat(ProtocolCellIdentityCodecV1.encode(pulsarCell()).toHex()).isEqualTo(PULSAR_NPC1_HEX);
        assertThat(ProtocolCellIdentityCodecV1.decode(hex(KAFKA_NPC1_HEX))).isEqualTo(kafkaCell());
        assertThat(ProtocolCellIdentityCodecV1.decode(hex(PULSAR_NPC1_HEX))).isEqualTo(pulsarCell());
    }

    @Test
    void rejectsWrongMagicUnknownCodeWrongLengthZeroIdentityAndTrailingBytes() {
        byte[] wrongMagic = hex(KAFKA_NPC1_HEX);
        wrongMagic[0] = 'X';
        byte[] unknownCode = hex(KAFKA_NPC1_HEX);
        unknownCode[4] = 0;
        unknownCode[5] = 3;
        byte[] zeroDeployment = hex(KAFKA_NPC1_HEX);
        Arrays.fill(zeroDeployment, 6, 22, (byte) 0);

        for (byte[] value : new byte[][] {
            wrongMagic,
            unknownCode,
            zeroDeployment,
            Arrays.copyOf(hex(KAFKA_NPC1_HEX), 37),
            Arrays.copyOf(hex(KAFKA_NPC1_HEX), 39),
            hex("4e5043310001")
        }) {
            assertThatThrownBy(() -> ProtocolCellIdentityCodecV1.decode(value))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
