/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.kafka.bookkeeper.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class KafkaPartitionFenceTransitionV1Test {
    @Test
    void rejectsPartitionIdentityChange() {
        KafkaPartitionProtocolStateV1 initial = KafkaProtocolStateFixtures.initialState();
        KafkaPartitionFenceV1 differentPartition = new KafkaPartitionFenceV1(
                initial.fence().bindingId(),
                initial.fence().topicIncarnation(),
                initial.fence().partitionId() + 1,
                initial.fence().bindingGeneration(),
                initial.fence().storageEpochId(),
                initial.fence().ownerEpoch() + 1,
                initial.fence().kafkaLeaderEpoch());

        assertThatThrownBy(() -> new KafkaPartitionFenceTransitionV1(
                        initial.fence(),
                        initial.stateVersion(),
                        differentPartition,
                        initial.frontiers(),
                        initial.references()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsOwnerOrKafkaLeaderEpochRegression() {
        KafkaPartitionProtocolStateV1 initial = KafkaProtocolStateFixtures.initialState();

        assertThatThrownBy(() -> new KafkaPartitionFenceTransitionV1(
                        initial.fence(),
                        initial.stateVersion(),
                        KafkaProtocolStateFixtures.fence(1, 2, 2, 4),
                        initial.frontiers(),
                        initial.references()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new KafkaPartitionFenceTransitionV1(
                        initial.fence(),
                        initial.stateVersion(),
                        KafkaProtocolStateFixtures.fence(1, 2, 3, 3),
                        initial.frontiers(),
                        initial.references()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsAChangeThatAdvancesNoFence() {
        KafkaPartitionProtocolStateV1 initial = KafkaProtocolStateFixtures.initialState();

        assertThatThrownBy(() -> new KafkaPartitionFenceTransitionV1(
                        initial.fence(),
                        initial.stateVersion(),
                        initial.fence(),
                        initial.frontiers(),
                        initial.references()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void transitionMayInstallAnElectionBoundedTruncatedStateAndNotifiesAfterCas() {
        KafkaPartitionProtocolStateV1 initial = new KafkaPartitionProtocolStateV1(
                KafkaProtocolStateFixtures.fence(1, 2, 3, 4),
                5,
                new KafkaPartitionFrontiersV1(0, 20, 20, 20, 18, 17),
                KafkaProtocolStateFixtures.references(3));
        List<KafkaPartitionPublicationEventV1> events = new ArrayList<>();
        KafkaPartitionPublicationCellV1 cell = new KafkaPartitionPublicationCellV1(initial, events::add);
        KafkaPartitionFenceTransitionV1 transition = new KafkaPartitionFenceTransitionV1(
                initial.fence(),
                initial.stateVersion(),
                KafkaProtocolStateFixtures.fence(1, 2, 4, 5),
                new KafkaPartitionFrontiersV1(0, 15, 15, 15, 12, 12),
                KafkaProtocolStateFixtures.references(4));

        KafkaPartitionPublicationResultV1 result = cell.transition(transition);

        assertThat(result.outcome()).isEqualTo(KafkaPartitionPublicationOutcomeV1.PUBLISHED);
        assertThat(cell.capture().stateVersion()).isEqualTo(6);
        assertThat(cell.capture().frontiers().readableEndOffset()).isEqualTo(15);
        assertThat(events)
                .singleElement()
                .extracting(KafkaPartitionPublicationEventV1::kind)
                .isEqualTo(KafkaPartitionPublicationKindV1.FENCE_TRANSITION);
    }

    @Test
    void staleTransitionCannotReplaceOrNotify() {
        KafkaPartitionProtocolStateV1 initial = KafkaProtocolStateFixtures.initialState();
        List<KafkaPartitionPublicationEventV1> events = new ArrayList<>();
        KafkaPartitionPublicationCellV1 cell = new KafkaPartitionPublicationCellV1(initial, events::add);
        KafkaPartitionFenceTransitionV1 stale = new KafkaPartitionFenceTransitionV1(
                initial.fence(),
                1,
                KafkaProtocolStateFixtures.fence(1, 2, 4, 5),
                initial.frontiers(),
                KafkaProtocolStateFixtures.references(1));

        assertThat(cell.transition(stale).outcome())
                .isEqualTo(KafkaPartitionPublicationOutcomeV1.STATE_VERSION_MISMATCH);
        assertThat(events).isEmpty();
    }

    @Test
    void transitionRejectsReferenceGenerationRollbackWithoutReplacingOrNotifying() {
        KafkaPartitionProtocolStateV1 initial = new KafkaPartitionProtocolStateV1(
                KafkaProtocolStateFixtures.fence(1, 2, 3, 4),
                5,
                new KafkaPartitionFrontiersV1(0, 20, 20, 20, 18, 17),
                KafkaProtocolStateFixtures.references(3));
        List<KafkaPartitionPublicationEventV1> events = new ArrayList<>();
        KafkaPartitionPublicationCellV1 cell = new KafkaPartitionPublicationCellV1(initial, events::add);
        KafkaPartitionFenceTransitionV1 transition = new KafkaPartitionFenceTransitionV1(
                initial.fence(),
                initial.stateVersion(),
                KafkaProtocolStateFixtures.fence(1, 2, 4, 5),
                new KafkaPartitionFrontiersV1(0, 15, 15, 15, 12, 12),
                KafkaProtocolStateFixtures.references(2));

        assertThat(cell.transition(transition).outcome())
                .isEqualTo(KafkaPartitionPublicationOutcomeV1.INVALID_FENCE_TRANSITION);
        assertThat(cell.capture()).isSameAs(initial);
        assertThat(events).isEmpty();
    }
}
