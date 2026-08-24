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
import java.util.Optional;
import org.junit.jupiter.api.Test;

class WalRunRuntimeTest {
    @Test
    void lanesInstantiateLazilyAndResolveIndependently() {
        WalRunRuntime runtime = new WalRunRuntime(ObjectWalControlTestFixtures.root(1, Optional.empty()));
        assertThat(runtime.instantiatedLaneCount()).isZero();

        LaneSequenceReservation latency = runtime.admitSealedPlan(plan(WalLaneId.OBJECT_LATENCY, 1), 101);
        LaneSequenceReservation cost = runtime.admitSealedPlan(plan(WalLaneId.OBJECT_COST, 2), 101);
        assertThat(runtime.instantiatedLaneCount()).isEqualTo(2);
        assertThat(latency.laneSequence()).isZero();
        assertThat(cost.laneSequence()).isZero();

        runtime.providerResolved(cost, 80);
        assertThat(runtime.resolvedVector()).isEqualTo(LaneSequenceVector.of(-1, -1, 0));
        runtime.providerResolved(latency, 70);
        assertThat(runtime.resolvedVector()).isEqualTo(LaneSequenceVector.of(0, -1, 0));
    }

    @Test
    void unresolvedBarrierAllowsOnlyExactSameCandidateRetry() {
        WalRunRuntime runtime = new WalRunRuntime(ObjectWalControlTestFixtures.root(1, Optional.empty()));
        LaneSequenceReservation reservation = runtime.admitSealedPlan(plan(WalLaneId.OBJECT_BALANCED, 3), 1);

        assertThatThrownBy(() -> runtime.admitSealedPlan(plan(WalLaneId.OBJECT_BALANCED, 4), 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("barrier");
        assertThat(runtime.retryReservation(
                        reservation.laneId(), reservation.laneSequence(), reservation.canonicalPlanSha256()))
                .isEqualTo(reservation);
        assertThatThrownBy(() -> runtime.retryReservation(
                        reservation.laneId(), reservation.laneSequence(), ObjectWalControlTestFixtures.digest(99)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void absentGapStopsWholeRunAndSealCannotSkipUnknown() {
        WalRunRuntime runtime = new WalRunRuntime(ObjectWalControlTestFixtures.root(1, Optional.empty()));
        LaneSequenceReservation latency = runtime.admitSealedPlan(plan(WalLaneId.OBJECT_LATENCY, 5), 1);
        LaneSequenceReservation cost = runtime.admitSealedPlan(plan(WalLaneId.OBJECT_COST, 6), 1);
        runtime.providerResolved(cost, 10);
        assertThatThrownBy(() -> {
                    runtime.stopAdmission(WalRunRuntime.StopReason.OWNER_REQUEST);
                    runtime.seal();
                })
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unresolved");

        runtime.providerAbsent(latency);
        assertThat(runtime.state()).isEqualTo(WalRunRuntime.State.STOPPING);
        assertThat(runtime.seal()).isEqualTo(LaneSequenceVector.of(-1, -1, 0));
        assertThatThrownBy(() -> runtime.admitSealedPlan(plan(WalLaneId.OBJECT_COST, 7), 2))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void longMaxSequenceIsAllocatedOnceAndThenRequiresSuccessor() {
        LaneSequenceAllocator allocator = new LaneSequenceAllocator(WalLaneId.OBJECT_COST, Long.MAX_VALUE - 1);
        LaneSequenceReservation maximum = allocator.allocate(plan(WalLaneId.OBJECT_COST, 8));
        assertThat(maximum.laneSequence()).isEqualTo(Long.MAX_VALUE);
        allocator.resolve(maximum);
        assertThat(allocator.resolvedThrough()).isEqualTo(Long.MAX_VALUE);
        assertThatThrownBy(() -> allocator.allocate(plan(WalLaneId.OBJECT_COST, 9)))
                .isInstanceOf(WalRunRuntime.SequenceExhaustedException.class);
    }

    @Test
    void resolvingInjectedMaximumStopsWholeRunBeforeCrossLaneAdmission() {
        WalRunRuntime runtime = new WalRunRuntime(ObjectWalControlTestFixtures.root(1, Optional.empty()), 2);
        for (int sequence = 0; sequence < 2; sequence++) {
            LaneSequenceReservation reservation =
                    runtime.admitSealedPlan(plan(WalLaneId.OBJECT_COST, 20 + sequence), 1);
            assertThat(reservation.laneSequence()).isEqualTo(sequence);
            runtime.providerResolved(reservation, 80);
        }
        LaneSequenceReservation maximum = runtime.admitSealedPlan(plan(WalLaneId.OBJECT_COST, 22), 1);
        assertThat(maximum.laneSequence()).isEqualTo(2);

        runtime.providerResolved(maximum, 80);

        assertThat(runtime.state()).isEqualTo(WalRunRuntime.State.STOPPING);
        assertThat(runtime.stopReason()).contains(WalRunRuntime.StopReason.SEQUENCE_EXHAUSTED);
        assertThatThrownBy(() -> runtime.admitSealedPlan(plan(WalLaneId.OBJECT_LATENCY, 23), 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no longer admits");
    }

    @Test
    void aggregateExtentLimitStopsBeforeAllocatingAnotherSequence() {
        WalRunRuntime runtime = new WalRunRuntime(ObjectWalControlTestFixtures.root(1, Optional.empty()));
        for (int sequence = 0; sequence < 100; sequence++) {
            LaneSequenceReservation reservation =
                    runtime.admitSealedPlan(plan(WalLaneId.OBJECT_LATENCY, 100 + sequence), 1);
            runtime.providerResolved(reservation, 80);
        }

        assertThatThrownBy(() -> runtime.admitSealedPlan(plan(WalLaneId.OBJECT_LATENCY, 201), 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("aggregate admission bound");
        assertThat(runtime.state()).isEqualTo(WalRunRuntime.State.STOPPING);
        assertThat(runtime.stopReason()).contains(WalRunRuntime.StopReason.RUN_LIMIT);
        assertThat(runtime.recoveryState().reservedExtentCount()).isEqualTo(100);
    }

    @Test
    void restoreRejectsUnrepresentableTerminalSequenceInsteadOfReopeningIt() {
        WalRunRuntime.RecoveredState impossible = new WalRunRuntime.RecoveredState(
                WalRunRuntime.State.STOPPING,
                Optional.of(WalRunRuntime.StopReason.SEQUENCE_EXHAUSTED),
                java.util.List.of(new WalRunRuntime.RecoveredLane(
                        WalLaneId.OBJECT_COST, Long.MAX_VALUE, Optional.empty(), false)),
                Long.MAX_VALUE,
                1,
                Long.MAX_VALUE,
                1);

        assertThatThrownBy(
                        () -> WalRunRuntime.restore(ObjectWalControlTestFixtures.root(1, Optional.empty()), impossible))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("successor required");
    }

    @Test
    void openRunRestorePreservesVectorCountersPendingIdentityAndDurableAgeBasis() {
        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalRunRuntime original = new WalRunRuntime(root);
        LaneSequenceReservation resolved = original.admitSealedPlan(plan(WalLaneId.OBJECT_LATENCY, 10), 1);
        original.providerResolved(resolved, 80);
        LaneSequenceReservation pending = original.admitSealedPlan(plan(WalLaneId.OBJECT_COST, 11), 1);

        WalRunRuntime restored = WalRunRuntime.restore(root, original.recoveryState());

        assertThat(restored.resolvedVector()).isEqualTo(LaneSequenceVector.of(0, -1, -1));
        assertThat(restored.retryReservation(pending.laneId(), pending.laneSequence(), pending.canonicalPlanSha256()))
                .isEqualTo(pending);
        assertThat(restored.resolvedExtentCount()).isEqualTo(1);
        assertThat(restored.instantiatedLaneCount()).isEqualTo(2);
        assertThatThrownBy(() -> restored.admitSealedPlan(plan(WalLaneId.OBJECT_BALANCED, 12), 60_000))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("age bound");
    }

    private static ImmutableExtentPlan plan(WalLaneId lane, int seed) {
        return new ImmutableExtentPlan(lane, 1, ObjectWalControlTestFixtures.digest(seed), 100);
    }
}
