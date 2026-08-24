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

package com.nereusstream.storage.object.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ObjectWalLeafKeyV1Test {
    @Test
    void exactLeafGrammarRoundTripsAtAllFixedWidthBoundaries() {
        ObjectProviderRootConfiguration provider =
                ObjectWalControlTestFixtures.root(1, Optional.empty()).providerConfiguration();
        ObjectWalLeafKeyV1 leaf = new ObjectWalLeafKeyV1(
                WalLaneId.OBJECT_COST,
                Long.MAX_VALUE,
                ObjectProviderRootConfiguration.FORMAT_MAX_PREFIX_BYTES,
                4_294_967_296L,
                ObjectWalControlTestFixtures.digest(7));

        assertThat(leaf.relativeKey()).hasSize(ObjectWalLeafKeyV1.RELATIVE_KEY_BYTES);
        assertThat(ObjectWalLeafKeyV1.parseRelative(leaf.relativeKey())).isEqualTo(leaf);
        assertThat(ObjectWalLeafKeyV1.parseFull(provider, leaf.fullKey(provider)))
                .isEqualTo(leaf);
        assertThat(leaf.fullKey(provider).getBytes(StandardCharsets.US_ASCII))
                .hasSize(ObjectWalLeafKeyV1.maximumFullKeyBytes(provider));
    }

    @Test
    void parserRejectsUnknownLaneWidthsCaseAndRootSubstitution() {
        ObjectProviderRootConfiguration provider =
                ObjectWalControlTestFixtures.root(1, Optional.empty()).providerConfiguration();
        ObjectWalLeafKeyV1 leaf =
                new ObjectWalLeafKeyV1(WalLaneId.OBJECT_LATENCY, 0, 256, 512, ObjectWalControlTestFixtures.digest(8));
        String exact = leaf.relativeKey();

        assertThatThrownBy(() -> ObjectWalLeafKeyV1.parseRelative("3" + exact.substring(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ObjectWalLeafKeyV1.parseRelative(exact.replace("0000000000000000000", "0")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ObjectWalLeafKeyV1.parseRelative(exact.toUpperCase(Locale.ROOT)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ObjectWalLeafKeyV1.parseFull(provider, "another-root/" + exact))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outside");
    }
}
