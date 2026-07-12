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

package com.nereusstream.managedledger.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

class F2L0RequestFactoryTest {
    private final F2L0RequestFactory requests = new F2L0RequestFactory();

    @Test
    void freezesEveryF2L0Option() {
        Duration timeout = Duration.ofSeconds(7);

        assertThat(requests.createOptions().profile()).isEqualTo(StorageProfile.OBJECT_WAL_SYNC_OBJECT);
        assertThat(requests.createOptions().attributes()).isEqualTo(Map.of(
                ManagedLedgerProjectionNames.PAYLOAD_MAPPING_ATTRIBUTE,
                ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1));

        assertThat(requests.appendOptions(timeout).appendSession()).isEmpty();
        assertThat(requests.appendOptions(timeout).durabilityLevel())
                .isEqualTo(DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED);
        assertThat(requests.appendOptions(timeout).timeout()).isSameAs(timeout);
        assertThat(requests.appendOptions(timeout).autoAcquireSession()).isTrue();
        assertThat(requests.appendOptions(timeout).tags()).isEmpty();

        assertThat(requests.recoveryOptions(timeout).timeout()).isSameAs(timeout);
        assertThat(requests.singleEntryReadOptions(1024, timeout).maxRecords()).isEqualTo(1);
        assertThat(requests.singleEntryReadOptions(1024, timeout).maxBytes()).isEqualTo(1024);
        assertThat(requests.singleEntryReadOptions(1024, timeout).isolation()).isEqualTo(ReadIsolation.COMMITTED);
        assertThat(requests.singleEntryReadOptions(1024, timeout).timeout()).isSameAs(timeout);
        assertThat(requests.sealOptions(timeout).reason()).isEqualTo(F2L0RequestFactory.TERMINATE_REASON);
        assertThat(requests.deleteOptions(timeout).reason()).isEqualTo(F2L0RequestFactory.DELETE_REASON);
    }

    @Test
    void delegatesInvalidBudgetRejectionToL0ValueContracts() {
        assertThatThrownBy(() -> requests.appendOptions(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> requests.recoveryOptions(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> requests.singleEntryReadOptions(0, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
