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
import org.junit.jupiter.api.Test;

class ProtocolKindV1Test {
    @Test
    void ownsTheExactClosedCodeTable() {
        assertThat(ProtocolKindV1.KAFKA.code()).isEqualTo(1);
        assertThat(ProtocolKindV1.PULSAR.code()).isEqualTo(2);
        assertThat(ProtocolKindV1.fromCode(1)).isSameAs(ProtocolKindV1.KAFKA);
        assertThat(ProtocolKindV1.fromCode(2)).isSameAs(ProtocolKindV1.PULSAR);
    }

    @Test
    void rejectsZeroUnknownAndOutOfRangeCodes() {
        for (int code : new int[] {-1, 0, 3, 65535, 65536}) {
            assertThatThrownBy(() -> ProtocolKindV1.fromCode(code)).isInstanceOf(IllegalArgumentException.class);
        }
    }
}
