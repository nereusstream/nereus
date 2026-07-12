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

package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ManagedLedgerProjectionNamesTest {
    @Test
    void matchesTheLockedDomainSeparatedGoldenVector() {
        String name = "tenant/ns/persistent/topic";

        assertThat(ManagedLedgerProjectionNames.managedLedgerNameHash(name))
                .isEqualTo("ugjdjmjjmrnhrunnrjqftfjyy62cvr2tsg5d5ps35t6c5xsexnuq");
        assertThat(ManagedLedgerProjectionNames.streamId(name, 1).value())
                .isEqualTo("s-uf6gggaiiw66rofdsii3n4jdckm2y26wr2zmabukunoszlplbg4a");
        assertThat(ManagedLedgerProjectionNames.streamId(name, 2))
                .isNotEqualTo(ManagedLedgerProjectionNames.streamId(name, 1));
    }

    @Test
    void validatesStrictUtf8AndTheExactEncodedLimit() {
        String exactLimit = "x".repeat(ManagedLedgerProjectionNames.MAX_MANAGED_LEDGER_NAME_BYTES);

        assertThat(ManagedLedgerProjectionNames.requireManagedLedgerName(exactLimit)).isSameAs(exactLimit);
        assertThatThrownBy(() -> ManagedLedgerProjectionNames.requireManagedLedgerName(" "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ManagedLedgerProjectionNames.requireManagedLedgerName("a\0b"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ManagedLedgerProjectionNames.requireManagedLedgerName("\uD800"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UTF-8");
        assertThatThrownBy(() -> ManagedLedgerProjectionNames.requireManagedLedgerName(exactLimit + "x"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNonPositiveIncarnations() {
        assertThatThrownBy(() -> ManagedLedgerProjectionNames.streamName("tenant/ns/topic", 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
