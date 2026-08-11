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

package com.nereusstream.domain.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class KafkaTopicNameTest {
    @Test
    void acceptsPinnedAlphabetAndExactLengthBoundary() {
        String maximum = "a".repeat(249);

        assertThat(new KafkaTopicName("Az09._-").bytes().toHex()).isEqualTo("417a30392e5f2d");
        assertThat(new KafkaTopicName(maximum).bytes().length()).isEqualTo(249);
        assertThat(KafkaTopicName.fromBytes("orders.v1".getBytes(StandardCharsets.US_ASCII))
                        .value())
                .isEqualTo("orders.v1");
    }

    @Test
    void rejectsReservedTooLongNonAsciiAndUnpairedSurrogateNames() {
        for (String value : new String[] {"", ".", "..", "a".repeat(250), "orders/a", "caf\u00e9", "x\ud800"}) {
            assertThatThrownBy(() -> new KafkaTopicName(value)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> KafkaTopicName.fromBytes(new byte[] {(byte) 0xff}))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
