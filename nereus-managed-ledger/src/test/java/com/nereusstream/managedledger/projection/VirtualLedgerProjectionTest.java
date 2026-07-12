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

import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import org.junit.jupiter.api.Test;

class VirtualLedgerProjectionTest {
    private static final String NAME = "tenant/ns/persistent/topic";

    @Test
    void validatesTheCompleteProjectionIdentity() {
        VirtualLedgerProjection projection = projection(1, VirtualLedgerProjection.MIN_VIRTUAL_LEDGER_ID);

        assertThat(projection.streamId()).isEqualTo(ManagedLedgerProjectionNames.streamId(NAME, 1));
        assertThat(projection.payloadMapping()).isEqualTo(ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1);
        projection.requireStorageClassBindingGeneration(1);
        assertThatThrownBy(() -> projection.requireStorageClassBindingGeneration(2))
                .isInstanceOf(ProjectionValidationException.class);

        assertThatThrownBy(() -> new VirtualLedgerProjection(
                new StreamId("wrong"), NAME, 1, 1,
                VirtualLedgerProjection.MIN_VIRTUAL_LEDGER_ID, 1,
                ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1, 0, 0))
                .isInstanceOf(ProjectionValidationException.class);
        assertThatThrownBy(() -> new VirtualLedgerProjection(
                ManagedLedgerProjectionNames.streamId(NAME, 1), NAME, 0, 1,
                VirtualLedgerProjection.MIN_VIRTUAL_LEDGER_ID, 1,
                ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1, 0, 0))
                .isInstanceOf(ProjectionValidationException.class);
        assertThatThrownBy(() -> new VirtualLedgerProjection(
                ManagedLedgerProjectionNames.streamId(NAME, 1), NAME, 1, 1,
                VirtualLedgerProjection.MIN_VIRTUAL_LEDGER_ID - 1, 1,
                ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1, 0, 0))
                .isInstanceOf(ProjectionValidationException.class);
        assertThatThrownBy(() -> new VirtualLedgerProjection(
                ManagedLedgerProjectionNames.streamId(NAME, 1), NAME, 1, 1,
                Long.MAX_VALUE, 1, ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1, 0, 0))
                .isInstanceOf(ProjectionValidationException.class);
        assertThatThrownBy(() -> new VirtualLedgerProjection(
                ManagedLedgerProjectionNames.streamId(NAME, 1), NAME, 1, 1,
                VirtualLedgerProjection.MIN_VIRTUAL_LEDGER_ID, 2,
                ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1, 0, 0))
                .isInstanceOf(ProjectionValidationException.class);
        assertThatThrownBy(() -> new VirtualLedgerProjection(
                ManagedLedgerProjectionNames.streamId(NAME, 1), NAME, 1, 1,
                VirtualLedgerProjection.MIN_VIRTUAL_LEDGER_ID, 1, "wrong", 0, 0))
                .isInstanceOf(ProjectionValidationException.class);
    }

    @Test
    void nextIncarnationHasASeparateStreamAndPositionNamespace() {
        VirtualLedgerProjection first = projection(1, VirtualLedgerProjection.MIN_VIRTUAL_LEDGER_ID);
        VirtualLedgerProjection second = projection(2, VirtualLedgerProjection.MIN_VIRTUAL_LEDGER_ID + 1);

        assertThat(second.incarnation()).isGreaterThan(first.incarnation());
        assertThat(second.streamId()).isNotEqualTo(first.streamId());
        assertThat(second.virtualLedgerId()).isNotEqualTo(first.virtualLedgerId());
    }

    private static VirtualLedgerProjection projection(long incarnation, long ledgerId) {
        return new VirtualLedgerProjection(
                ManagedLedgerProjectionNames.streamId(NAME, incarnation),
                NAME,
                1,
                incarnation,
                ledgerId,
                VirtualLedgerProjection.MAPPING_VERSION,
                ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1,
                0,
                0);
    }
}
