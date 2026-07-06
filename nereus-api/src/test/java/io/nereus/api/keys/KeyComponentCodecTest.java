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

package io.nereus.api.keys;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class KeyComponentCodecTest {
    @Test
    void rawComponentsAllowOnlyStableAsciiPathSegments() {
        assertThat(KeyComponentCodec.encodeComponent("tenant_ns.topic-0"))
                .isEqualTo("tenant_ns.topic-0");
    }

    @Test
    void unsafeComponentsUseBase32LowerNoPadWithReservedPrefix() {
        assertThat(KeyComponentCodec.encodeComponent("tenant/ns"))
                .isEqualTo("b32-orsw4ylooqxw44y");
        assertThat(KeyComponentCodec.encodeComponent("."))
                .isEqualTo("b32-fy");
        assertThat(KeyComponentCodec.encodeComponent(".."))
                .isEqualTo("b32-fyxa");
        assertThat(KeyComponentCodec.encodeComponent("b32-raw"))
                .isEqualTo("b32-miztellsmf3q");
        assertThat(KeyComponentCodec.encodeComponent("C:"))
                .isEqualTo("b32-im5a");
    }

    @Test
    void nonNegativeLongUsesLexicographicallySortableFixedWidthDecimal() {
        assertThat(KeyComponentCodec.encodeNonNegativeLong(0))
                .isEqualTo("0000000000000000000");
        assertThat(KeyComponentCodec.encodeNonNegativeLong(9))
                .isLessThan(KeyComponentCodec.encodeNonNegativeLong(10));
        assertThat(KeyComponentCodec.encodeNonNegativeLong(10))
                .isLessThan(KeyComponentCodec.encodeNonNegativeLong(100));
        assertThat(KeyComponentCodec.encodeNonNegativeLong(Long.MAX_VALUE))
                .isEqualTo("9223372036854775807");
        assertThatThrownBy(() -> KeyComponentCodec.encodeNonNegativeLong(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
