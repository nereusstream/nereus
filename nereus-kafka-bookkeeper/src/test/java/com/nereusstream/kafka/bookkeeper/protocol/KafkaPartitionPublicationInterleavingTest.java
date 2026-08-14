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
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class KafkaPartitionPublicationInterleavingTest {
    @Test
    void fenceWinningFirstMakesTheOldCommitStaleWithoutCommitWakeup() {
        KafkaPartitionProtocolStateV1 initial = KafkaProtocolStateFixtures.initialState();
        List<KafkaPartitionPublicationEventV1> events = new ArrayList<>();
        KafkaPartitionPublicationCellV1 cell = new KafkaPartitionPublicationCellV1(initial, events::add);
        KafkaPartitionCommitSlotV1 commit = KafkaProtocolStateFixtures.commitSlot(initial, 0, 10, 1);
        KafkaPartitionFenceTransitionV1 fence = transition(initial);
        AtomicReference<KafkaPartitionPublicationResultV1> commitResult = new AtomicReference<>();
        DeterministicInterleavingScheduler scheduler = new DeterministicInterleavingScheduler();
        scheduler.schedule("commit", () -> commitResult.set(cell.publish(commit)));
        scheduler.schedule("fence", () -> cell.transition(fence));

        scheduler.run("fence");
        scheduler.run("commit");

        assertThat(commitResult.get().outcome()).isEqualTo(KafkaPartitionPublicationOutcomeV1.FENCE_MISMATCH);
        assertThat(events)
                .singleElement()
                .extracting(KafkaPartitionPublicationEventV1::kind)
                .isEqualTo(KafkaPartitionPublicationKindV1.FENCE_TRANSITION);
        assertThat(scheduler.pendingCount()).isZero();
    }

    @Test
    void commitWinningFirstMakesTheOldTransitionStaleAndWakesExactlyOnce() {
        KafkaPartitionProtocolStateV1 initial = KafkaProtocolStateFixtures.initialState();
        List<KafkaPartitionPublicationEventV1> events = new ArrayList<>();
        KafkaPartitionPublicationCellV1 cell = new KafkaPartitionPublicationCellV1(initial, events::add);
        KafkaPartitionCommitSlotV1 commit = KafkaProtocolStateFixtures.commitSlot(initial, 0, 10, 1);
        KafkaPartitionFenceTransitionV1 fence = transition(initial);
        AtomicReference<KafkaPartitionPublicationResultV1> fenceResult = new AtomicReference<>();
        DeterministicInterleavingScheduler scheduler = new DeterministicInterleavingScheduler();
        scheduler.schedule("commit", () -> cell.publish(commit));
        scheduler.schedule("fence", () -> fenceResult.set(cell.transition(fence)));

        scheduler.run("commit");
        scheduler.run("fence");

        assertThat(fenceResult.get().outcome()).isEqualTo(KafkaPartitionPublicationOutcomeV1.STATE_VERSION_MISMATCH);
        assertThat(events)
                .singleElement()
                .extracting(KafkaPartitionPublicationEventV1::kind)
                .isEqualTo(KafkaPartitionPublicationKindV1.COMMIT);
    }

    @Test
    void laterDurableSlotCannotPublishAroundItsPredecessorGap() {
        KafkaPartitionProtocolStateV1 initial = KafkaProtocolStateFixtures.initialState();
        List<KafkaPartitionPublicationEventV1> events = new ArrayList<>();
        KafkaPartitionPublicationCellV1 cell = new KafkaPartitionPublicationCellV1(initial, events::add);
        KafkaPartitionCommitSlotV1 first = KafkaProtocolStateFixtures.commitSlot(initial, 0, 10, 1);
        KafkaPartitionProtocolStateV1 expectedAfterFirst = new KafkaPartitionProtocolStateV1(
                initial.fence(), 1, first.replacementFrontiers(), first.replacementReferences());
        KafkaPartitionCommitSlotV1 second = KafkaProtocolStateFixtures.commitSlot(expectedAfterFirst, 10, 20, 2);

        assertThat(cell.publish(second).outcome()).isEqualTo(KafkaPartitionPublicationOutcomeV1.STATE_VERSION_MISMATCH);
        assertThat(events).isEmpty();
        assertThat(cell.publish(first).published()).isTrue();
        assertThat(cell.publish(second).published()).isTrue();
        assertThat(cell.capture().frontiers().readableEndOffset()).isEqualTo(20);
        assertThat(events).hasSize(2);
    }

    @Test
    void capturesOnlyWholeBeforeOrAfterRootsAcrossPublication() {
        KafkaPartitionProtocolStateV1 initial = KafkaProtocolStateFixtures.initialState();
        KafkaPartitionPublicationCellV1 cell = new KafkaPartitionPublicationCellV1(initial, event -> {});
        KafkaPartitionCommitSlotV1 commit = KafkaProtocolStateFixtures.commitSlot(initial, 0, 10, 1);
        List<KafkaPartitionReadSnapshotV1> snapshots = new ArrayList<>();
        DeterministicInterleavingScheduler scheduler = new DeterministicInterleavingScheduler();
        scheduler.schedule("capture-before", () -> snapshots.add(cell.captureReadSnapshot()));
        scheduler.schedule("publish", () -> cell.publish(commit));
        scheduler.schedule("capture-after", () -> snapshots.add(cell.captureReadSnapshot()));

        scheduler.run("capture-before");
        scheduler.run("publish");
        scheduler.run("capture-after");

        assertThat(snapshots.get(0)).isSameAs(initial);
        assertThat(snapshots.get(1)).isSameAs(cell.capture());
        assertThat(snapshots.get(0).stateVersion()).isZero();
        assertThat(snapshots.get(1).stateVersion()).isEqualTo(1);
    }

    private static KafkaPartitionFenceTransitionV1 transition(KafkaPartitionProtocolStateV1 initial) {
        return new KafkaPartitionFenceTransitionV1(
                initial.fence(),
                initial.stateVersion(),
                KafkaProtocolStateFixtures.fence(1, 2, 4, 5),
                initial.frontiers(),
                KafkaProtocolStateFixtures.references(1));
    }
}
