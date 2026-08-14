/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.kafka.bookkeeper.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class KafkaPartitionPublicationCellV1Test {
    @Test
    void contiguousCommitPublishesOneCoherentRootThenNotifies() {
        KafkaPartitionProtocolStateV1 initial = KafkaProtocolStateFixtures.initialState();
        List<KafkaPartitionPublicationEventV1> events = new ArrayList<>();
        KafkaPartitionPublicationCellV1 cell = new KafkaPartitionPublicationCellV1(initial, events::add);

        KafkaPartitionPublicationResultV1 result =
                cell.publish(KafkaProtocolStateFixtures.commitSlot(initial, 0, 10, 1));

        assertThat(result.outcome()).isEqualTo(KafkaPartitionPublicationOutcomeV1.PUBLISHED);
        assertThat(result.observedState()).isSameAs(cell.capture());
        assertThat(cell.capture().stateVersion()).isEqualTo(1);
        assertThat(cell.capture().frontiers().readableEndOffset()).isEqualTo(10);
        assertThat(events).singleElement().satisfies(event -> {
            assertThat(event.kind()).isEqualTo(KafkaPartitionPublicationKindV1.COMMIT);
            assertThat(event.previous()).isSameAs(initial);
            assertThat(event.published()).isSameAs(cell.capture());
        });
    }

    @Test
    void nonContiguousCommitCannotPublishOrWake() {
        KafkaPartitionProtocolStateV1 initial = KafkaProtocolStateFixtures.initialState();
        List<KafkaPartitionPublicationEventV1> events = new ArrayList<>();
        KafkaPartitionPublicationCellV1 cell = new KafkaPartitionPublicationCellV1(initial, events::add);

        KafkaPartitionPublicationResultV1 result =
                cell.publish(KafkaProtocolStateFixtures.commitSlot(initial, 1, 10, 1));

        assertThat(result.outcome()).isEqualTo(KafkaPartitionPublicationOutcomeV1.NON_CONTIGUOUS_COMMIT);
        assertThat(cell.capture()).isSameAs(initial);
        assertThat(events).isEmpty();
    }

    @Test
    void exactFenceMismatchCannotPublishOrWake() {
        KafkaPartitionProtocolStateV1 initial = KafkaProtocolStateFixtures.initialState();
        List<KafkaPartitionPublicationEventV1> events = new ArrayList<>();
        KafkaPartitionPublicationCellV1 cell = new KafkaPartitionPublicationCellV1(initial, events::add);
        KafkaPartitionCommitSlotV1 valid = KafkaProtocolStateFixtures.commitSlot(initial, 0, 10, 1);
        KafkaPartitionCommitSlotV1 wrongFence = new KafkaPartitionCommitSlotV1(
                KafkaProtocolStateFixtures.fence(1, 2, 4, 4),
                valid.predecessorStateVersion(),
                valid.commitStartOffset(),
                valid.commitEndOffset(),
                valid.replacementFrontiers(),
                valid.replacementReferences());

        assertThat(cell.publish(wrongFence).outcome()).isEqualTo(KafkaPartitionPublicationOutcomeV1.FENCE_MISMATCH);
        assertThat(events).isEmpty();
    }

    @Test
    void predecessorVersionMismatchCannotPublishOrWake() {
        KafkaPartitionProtocolStateV1 initial = KafkaProtocolStateFixtures.initialState();
        List<KafkaPartitionPublicationEventV1> events = new ArrayList<>();
        KafkaPartitionPublicationCellV1 cell = new KafkaPartitionPublicationCellV1(initial, events::add);
        KafkaPartitionCommitSlotV1 valid = KafkaProtocolStateFixtures.commitSlot(initial, 0, 10, 1);
        KafkaPartitionCommitSlotV1 stale = new KafkaPartitionCommitSlotV1(
                valid.expectedFence(),
                1,
                valid.commitStartOffset(),
                valid.commitEndOffset(),
                valid.replacementFrontiers(),
                valid.replacementReferences());

        assertThat(cell.publish(stale).outcome()).isEqualTo(KafkaPartitionPublicationOutcomeV1.STATE_VERSION_MISMATCH);
        assertThat(events).isEmpty();
    }

    @Test
    void commitRejectsReferenceOrFrontierRegressionAndRequiresHiddenTailAdvance() {
        KafkaPartitionProtocolStateV1 initial = new KafkaPartitionProtocolStateV1(
                KafkaProtocolStateFixtures.fence(1, 2, 3, 4),
                2,
                new KafkaPartitionFrontiersV1(0, 20, 10, 10, 8, 7),
                KafkaProtocolStateFixtures.references(2));
        KafkaPartitionPublicationCellV1 cell = new KafkaPartitionPublicationCellV1(initial, event -> {});
        KafkaPartitionCommitSlotV1 invalid = new KafkaPartitionCommitSlotV1(
                initial.fence(),
                initial.stateVersion(),
                10,
                15,
                new KafkaPartitionFrontiersV1(0, 20, 15, 15, 8, 7),
                KafkaProtocolStateFixtures.references(2));

        assertThat(cell.publish(invalid).outcome())
                .isEqualTo(KafkaPartitionPublicationOutcomeV1.INVALID_COMMIT_REPLACEMENT);
        assertThat(cell.capture()).isSameAs(initial);
    }

    @Test
    void notificationFailureIsClosedAsPublishedAndNeverRollsBackState() {
        KafkaPartitionProtocolStateV1 initial = KafkaProtocolStateFixtures.initialState();
        KafkaPartitionPublicationCellV1 cell = new KafkaPartitionPublicationCellV1(initial, event -> {
            throw new IllegalStateException("waiter queue unavailable");
        });

        KafkaPartitionPublicationResultV1 result =
                cell.publish(KafkaProtocolStateFixtures.commitSlot(initial, 0, 10, 1));

        assertThat(result.outcome()).isEqualTo(KafkaPartitionPublicationOutcomeV1.PUBLISHED_NOTIFICATION_FAILED);
        assertThat(result.published()).isTrue();
        assertThat(cell.capture().frontiers().readableEndOffset()).isEqualTo(10);
    }
}
