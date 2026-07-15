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

package com.nereusstream.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class F4ApiValueTest {
    @Test
    void readViewUsesClosedDurableWireIds() {
        assertThat(ReadView.COMMITTED.wireId()).isEqualTo(1);
        assertThat(ReadView.TOPIC_COMPACTED.wireId()).isEqualTo(2);
        assertThat(ReadView.fromWireId(1)).isEqualTo(ReadView.COMMITTED);
        assertThat(ReadView.fromWireId(2)).isEqualTo(ReadView.TOPIC_COMPACTED);
        assertThatThrownBy(() -> ReadView.fromWireId(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void generationRejectsNegativeValues() {
        assertThat(new GenerationId(0).value()).isZero();
        assertThatThrownBy(() -> new GenerationId(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void publicationIdRequiresAtLeast128BitsOfCanonicalBase32() {
        assertThat(new PublicationId("abcdefghijklmnopqrstuvwxyz").value())
                .isEqualTo("abcdefghijklmnopqrstuvwxyz");
        assertThatThrownBy(() -> new PublicationId("abcdefghijklmnopqrstuvwxy"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PublicationId("ABCDEFGHIJKLMNOPQRSTUVWXY2"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PublicationId("abcdefghijklmnopqrstuvwxy1"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void objectKeyHashIsExactSha256Base32OfObjectKey() {
        ObjectKeyHash hash = ObjectKeyHash.from(new ObjectKey("cluster/objects/example"));

        assertThat(hash.value())
                .isEqualTo("6zx5cifhslcnhcpnpbwd3b5gqut4yebpefgn3i5n6fa5wyp2xw6q");
        assertThatThrownBy(() -> new ObjectKeyHash(hash.value().substring(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ObjectKeyHash(hash.value().toUpperCase()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
