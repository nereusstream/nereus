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

import static com.nereusstream.domain.DomainTestFixtures.KAFKA_NTI1_HEX;
import static com.nereusstream.domain.DomainTestFixtures.PULSAR_NTI1_HEX;
import static com.nereusstream.domain.DomainTestFixtures.hex;
import static com.nereusstream.domain.DomainTestFixtures.kafkaIncarnation;
import static com.nereusstream.domain.DomainTestFixtures.pulsarIncarnation;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.protocol.PulsarBindingGeneration;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class TopicIncarnationIdentityCodecV1Test {
    @Test
    void encodesRawKafkaUuidAndLiteralKafkaAndPulsarGoldens() {
        assertThat(TopicIncarnationIdentityCodecV1.encode(kafkaIncarnation()).toHex())
                .isEqualTo(KAFKA_NTI1_HEX);
        assertThat(TopicIncarnationIdentityCodecV1.encode(pulsarIncarnation()).toHex())
                .isEqualTo(PULSAR_NTI1_HEX);
        assertThat(TopicIncarnationIdentityCodecV1.decode(hex(KAFKA_NTI1_HEX))).isEqualTo(kafkaIncarnation());
        assertThat(TopicIncarnationIdentityCodecV1.decode(hex(PULSAR_NTI1_HEX))).isEqualTo(pulsarIncarnation());
        assertThat(KAFKA_NTI1_HEX).contains("404142434445464748494a4b4c4d4e4f");
    }

    @Test
    void rejectsUnsignedLengthOverflowUnderflowAndTrailingBytesBeforeAllocation() {
        byte[] unsignedOverflow = hex(KAFKA_NTI1_HEX);
        Arrays.fill(unsignedOverflow, 22, 26, (byte) 0xff);
        byte[] declaredTooLong = hex(KAFKA_NTI1_HEX);
        declaredTooLong[25] = 10;

        for (byte[] value : new byte[][] {
            unsignedOverflow,
            declaredTooLong,
            Arrays.copyOf(hex(KAFKA_NTI1_HEX), 24),
            Arrays.copyOf(hex(KAFKA_NTI1_HEX), hex(KAFKA_NTI1_HEX).length + 1)
        }) {
            assertThatThrownBy(() -> TopicIncarnationIdentityCodecV1.decode(value))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsMalformedUtf8ZeroGenerationReservedUuidMagicAndCode() {
        byte[] malformedUtf8 = hex(PULSAR_NTI1_HEX);
        malformedUtf8[10] = (byte) 0xc0;
        malformedUtf8[11] = (byte) 0xaf;
        byte[] zeroGeneration = hex(PULSAR_NTI1_HEX);
        Arrays.fill(zeroGeneration, zeroGeneration.length - Long.BYTES, zeroGeneration.length, (byte) 0);
        byte[] reservedUuid = hex(KAFKA_NTI1_HEX);
        Arrays.fill(reservedUuid, 6, 22, (byte) 0);
        reservedUuid[21] = 1;
        byte[] wrongMagic = hex(KAFKA_NTI1_HEX);
        wrongMagic[3] = 'X';
        byte[] wrongCode = hex(KAFKA_NTI1_HEX);
        wrongCode[5] = 3;

        for (byte[] value : new byte[][] {malformedUtf8, zeroGeneration, reservedUuid, wrongMagic, wrongCode}) {
            assertThatThrownBy(() -> TopicIncarnationIdentityCodecV1.decode(value))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void generationIncrementFailsClosedAtSignedLongMaximum() {
        assertThat(new PulsarBindingGeneration(41).next()).isEqualTo(new PulsarBindingGeneration(42));
        assertThatThrownBy(() -> new PulsarBindingGeneration(Long.MAX_VALUE).next())
                .isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> new PulsarBindingGeneration(0)).isInstanceOf(IllegalArgumentException.class);
    }
}
