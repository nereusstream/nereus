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

package com.nereusstream.domain.bytes;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.junit.jupiter.api.Test;

class CanonicalUtf8Test {
    @Test
    void strictlyRoundTripsAndDoesNotNormalize() {
        CanonicalUtf8 composed = CanonicalUtf8.fromString("\u00e9");
        CanonicalUtf8 decomposed = CanonicalUtf8.fromString("e\u0301");

        assertThat(CanonicalUtf8.fromBytes(composed.bytes().toByteArray())).isEqualTo(composed);
        assertThat(composed).isNotEqualTo(decomposed);
        assertThat(composed.bytes().toHex()).isEqualTo("c3a9");
        assertThat(decomposed.bytes().toHex()).isEqualTo("65cc81");
    }

    @Test
    void rejectsMalformedTruncatedOverlongAndUnpairedInputs() {
        for (byte[] bytes : new byte[][] {
            {(byte) 0xc0, (byte) 0xaf},
            {(byte) 0xe2, (byte) 0x82},
            {(byte) 0x80},
            {(byte) 0xed, (byte) 0xa0, (byte) 0x80}
        }) {
            assertThatThrownBy(() -> CanonicalUtf8.fromBytes(bytes)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> CanonicalUtf8.fromString("x\ud800")).isInstanceOf(IllegalArgumentException.class);
    }
}
