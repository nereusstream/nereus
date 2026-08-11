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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.protocol.PulsarBindingGeneration;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import org.junit.jupiter.api.Test;

class PulsarAuthorityLeafCodecV1Test {
    private static final PulsarPersistenceName NAME = PulsarPersistenceName.fromString("persistent://tenant/ns/orders");
    private static final String DIGEST = "9c568f34755f91b5f6672b77d02ccd35c34eddb2622a5329abbd0d3291436ce2";

    @Test
    void freezesNpn1DigestAndLowercaseLeafGrammar() {
        assertThat(PulsarAuthorityLeafCodecV1.nameDigestPreimage(NAME).toHex())
                .isEqualTo("4e504e310000001d70657273697374656e743a2f2f74656e616e742f6e732f6f7264657273");
        assertThat(PulsarAuthorityLeafCodecV1.nameDigest(NAME).toHex()).isEqualTo(DIGEST);
        assertThat(PulsarAuthorityLeafCodecV1.selectorLeaf(NAME)).isEqualTo(DIGEST);
        assertThat(PulsarAuthorityLeafCodecV1.aggregateLeaf(NAME, new PulsarBindingGeneration(42)))
                .isEqualTo(DIGEST + "/0000000000000000042");
    }

    @Test
    void formatsBothGenerationBoundariesAtExactlyNineteenDigits() {
        assertThat(PulsarAuthorityLeafCodecV1.generation19(new PulsarBindingGeneration(1)))
                .isEqualTo("0000000000000000001");
        assertThat(PulsarAuthorityLeafCodecV1.generation19(new PulsarBindingGeneration(Long.MAX_VALUE)))
                .isEqualTo("9223372036854775807");
    }

    @Test
    void rejectsLeafNameGenerationCaseAndGrammarMismatches() {
        PulsarBindingGeneration generation = new PulsarBindingGeneration(42);
        PulsarAuthorityLeafCodecV1.requireSelectorLeaf(DIGEST, NAME);
        PulsarAuthorityLeafCodecV1.requireAggregateLeaf(DIGEST + "/0000000000000000042", NAME, generation);

        assertThatThrownBy(() -> PulsarAuthorityLeafCodecV1.requireSelectorLeaf(DIGEST.toUpperCase(), NAME))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PulsarAuthorityLeafCodecV1.requireAggregateLeaf(
                        DIGEST + "/0000000000000000041", NAME, generation))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PulsarAuthorityLeafCodecV1.requireSelectorLeaf(
                        DIGEST, PulsarPersistenceName.fromString("persistent://tenant/ns/other")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
