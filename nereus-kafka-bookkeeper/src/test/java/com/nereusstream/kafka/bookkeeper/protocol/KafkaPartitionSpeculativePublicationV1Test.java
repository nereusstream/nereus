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

package com.nereusstream.kafka.bookkeeper.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class KafkaPartitionSpeculativePublicationV1Test {
    @Test
    void speculativeAdmissionAdvancesOnlyAllocatedAndHiddenQueueReferenceWithoutWakeup() {
        KafkaPartitionProtocolStateV1 initial = KafkaProtocolStateFixtures.initialState();
        List<KafkaPartitionPublicationEventV1> events = new ArrayList<>();
        KafkaPartitionPublicationCellV1 cell = new KafkaPartitionPublicationCellV1(initial, events::add);

        KafkaPartitionPublicationResultV1 result = cell.stageSpeculative(slot(initial, 0, 10, 1));

        assertThat(result.outcome()).isEqualTo(KafkaPartitionPublicationOutcomeV1.PUBLISHED);
        assertThat(cell.capture().frontiers()).isEqualTo(new KafkaPartitionFrontiersV1(0, 10, 0, 0, 0, 0));
        assertThat(cell.capture().references().speculativeProducerQueue().generation())
                .isEqualTo(1);
        assertThat(cell.capture().references().activeTail())
                .isEqualTo(initial.references().activeTail());
        assertThat(events).isEmpty();
    }

    @Test
    void nonContiguousAllocationCannotPublish() {
        KafkaPartitionProtocolStateV1 initial = KafkaProtocolStateFixtures.initialState();
        KafkaPartitionPublicationCellV1 cell = new KafkaPartitionPublicationCellV1(initial, event -> {});

        assertThat(cell.stageSpeculative(slot(initial, 1, 10, 1)).outcome())
                .isEqualTo(KafkaPartitionPublicationOutcomeV1.NON_CONTIGUOUS_ALLOCATION);
        assertThat(cell.capture()).isSameAs(initial);
    }

    @Test
    void speculativeAdmissionCannotChangeAnyOtherComponentReference() {
        KafkaPartitionProtocolStateV1 initial = KafkaProtocolStateFixtures.initialState();
        KafkaPartitionPublicationCellV1 cell = new KafkaPartitionPublicationCellV1(initial, event -> {});
        KafkaPartitionStateReferencesV1 valid = replaceSpeculative(initial.references(), 1);
        KafkaPartitionStateReferencesV1 invalid = new KafkaPartitionStateReferencesV1(
                valid.runTable(),
                valid.activeTail(),
                valid.sourceMap(),
                KafkaProtocolStateFixtures.reference(1, 44),
                valid.speculativeProducerQueue(),
                valid.transactionIndex(),
                valid.leaderEpochIndex(),
                valid.checkpointVector(),
                valid.sourceProtection());
        KafkaPartitionSpeculativeSlotV1 slot = new KafkaPartitionSpeculativeSlotV1(initial.fence(), 0, 0, 10, invalid);

        assertThat(cell.stageSpeculative(slot).outcome())
                .isEqualTo(KafkaPartitionPublicationOutcomeV1.INVALID_SPECULATIVE_REPLACEMENT);
    }

    @Test
    void exactFenceStillProtectsSpeculativeAdmission() {
        KafkaPartitionProtocolStateV1 initial = KafkaProtocolStateFixtures.initialState();
        KafkaPartitionPublicationCellV1 cell = new KafkaPartitionPublicationCellV1(initial, event -> {});
        KafkaPartitionSpeculativeSlotV1 wrongFence = new KafkaPartitionSpeculativeSlotV1(
                KafkaProtocolStateFixtures.fence(1, 2, 4, 4), 0, 0, 10, replaceSpeculative(initial.references(), 1));

        assertThat(cell.stageSpeculative(wrongFence).outcome())
                .isEqualTo(KafkaPartitionPublicationOutcomeV1.FENCE_MISMATCH);
    }

    @Test
    void twoSlotsBuiltFromOnePredecessorCannotBothWin() {
        KafkaPartitionProtocolStateV1 initial = KafkaProtocolStateFixtures.initialState();
        KafkaPartitionPublicationCellV1 cell = new KafkaPartitionPublicationCellV1(initial, event -> {});
        KafkaPartitionSpeculativeSlotV1 first = slot(initial, 0, 10, 1);
        KafkaPartitionSpeculativeSlotV1 competing = slot(initial, 0, 5, 1);

        assertThat(cell.stageSpeculative(first).published()).isTrue();
        assertThat(cell.stageSpeculative(competing).outcome())
                .isEqualTo(KafkaPartitionPublicationOutcomeV1.STATE_VERSION_MISMATCH);
    }

    private static KafkaPartitionSpeculativeSlotV1 slot(
            KafkaPartitionProtocolStateV1 initial, long start, long end, long generation) {
        return new KafkaPartitionSpeculativeSlotV1(
                initial.fence(),
                initial.stateVersion(),
                start,
                end,
                replaceSpeculative(initial.references(), generation));
    }

    private static KafkaPartitionStateReferencesV1 replaceSpeculative(
            KafkaPartitionStateReferencesV1 references, long generation) {
        return new KafkaPartitionStateReferencesV1(
                references.runTable(),
                references.activeTail(),
                references.sourceMap(),
                references.committedProducerState(),
                KafkaProtocolStateFixtures.reference(generation, 77),
                references.transactionIndex(),
                references.leaderEpochIndex(),
                references.checkpointVector(),
                references.sourceProtection());
    }
}
