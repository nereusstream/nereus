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

package com.nereusstream.storage.object.recovery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class CumulativeRecoveryBudgetTest {
    @Test
    void fallbackAndRetryUseTheSameMonotonicCounters() {
        AtomicLong clock = new AtomicLong(100);
        CumulativeRecoveryBudget budget = new CumulativeRecoveryBudget(limits(2, 1000), clock::get);
        budget.chargeListPage(1, 10);
        CumulativeRecoveryBudget.Snapshot beforeFallback = budget.enterFallback();
        budget.chargeRetry();
        budget.chargeListPage(1, 10);

        assertThat(beforeFallback.listPages()).isEqualTo(1);
        assertThat(budget.snapshot().listPages()).isEqualTo(2);
        assertThat(budget.snapshot().retryAttempts()).isEqualTo(1);
        assertThatThrownBy(() -> budget.chargeListPage(1, 10))
                .isInstanceOf(RecoveryEnvelopeExceededException.class)
                .hasMessageContaining("LIST pages");
    }

    @Test
    void memoryConcurrencyAndWallTimeExhaustionFailClosed() {
        AtomicLong clock = new AtomicLong();
        CumulativeRecoveryBudget budget = new CumulativeRecoveryBudget(limits(3, 10), clock::get);
        budget.acquireWorkingSet(99);
        CumulativeRecoveryBudget.Snapshot beforeRejectedAcquire = budget.snapshot();
        assertThatThrownBy(() -> budget.acquireWorkingSet(1))
                .isInstanceOf(RecoveryEnvelopeExceededException.class)
                .hasMessageContaining("concurrency");
        assertThat(budget.snapshot().workingMemoryBytes()).isEqualTo(beforeRejectedAcquire.workingMemoryBytes());
        assertThat(budget.snapshot().currentConcurrency()).isEqualTo(beforeRejectedAcquire.currentConcurrency());
        budget.releaseWorkingSet(99);
        clock.set(11);
        assertThatThrownBy(budget::snapshot)
                .isInstanceOf(RecoveryEnvelopeExceededException.class)
                .hasMessageContaining("wall time");
    }

    @Test
    void monotonicClockRegressionFailsClosed() {
        AtomicLong clock = new AtomicLong(100);
        CumulativeRecoveryBudget budget = new CumulativeRecoveryBudget(limits(3, 10), clock::get);

        clock.set(99);

        assertThatThrownBy(budget::snapshot)
                .isInstanceOf(RecoveryEnvelopeExceededException.class)
                .hasMessageContaining("clock regression");
    }

    @Test
    void monotonicClockSubtractionOverflowFailsClosed() {
        AtomicLong clock = new AtomicLong(Long.MIN_VALUE);
        CumulativeRecoveryBudget budget = new CumulativeRecoveryBudget(limits(3, Long.MAX_VALUE), clock::get);

        clock.set(Long.MAX_VALUE);

        assertThatThrownBy(budget::snapshot)
                .isInstanceOf(RecoveryEnvelopeExceededException.class)
                .hasMessageContaining("arithmetic overflow");
    }

    @Test
    void listReservationClampsToRemainderAndSettlesOnlyAfterSuccess() {
        CumulativeRecoveryBudget budget = new CumulativeRecoveryBudget(limits(2, 1000), () -> 0);
        budget.chargeListPage(1, 1);

        CumulativeRecoveryBudget.ListReservation reservation = budget.reserveList(10, 100, 10_000);

        assertThat(reservation.maximumPages()).isEqualTo(1);
        assertThat(reservation.maximumKeys()).isEqualTo(9);
        assertThat(reservation.maximumCanonicalKeyBytes()).isEqualTo(1023);
        assertThat(budget.snapshot().listPages()).isEqualTo(2);
        reservation.settle(1, 2, 20);
        assertThat(budget.snapshot().listedKeys()).isEqualTo(3);
        assertThat(budget.snapshot().listedKeyBytes()).isEqualTo(21);
        assertThatThrownBy(() -> reservation.settle(1, 2, 20))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("twice");
        assertThatThrownBy(() -> budget.reserveList(1, 1, 1))
                .isInstanceOf(RecoveryEnvelopeExceededException.class)
                .hasMessageContaining("LIST pages");
    }

    @Test
    void compositeRequestChargeFailsAtomicallyBeforeNetworkAdmission() {
        CumulativeRecoveryBudget budget = new CumulativeRecoveryBudget(limits(3, 1000), () -> 0);

        assertThatThrownBy(() -> budget.chargeFullGet(4097))
                .isInstanceOf(RecoveryEnvelopeExceededException.class)
                .hasMessageContaining("canonical body bytes");

        assertThat(budget.snapshot().fullGetRequests()).isZero();
        assertThat(budget.snapshot().canonicalBodyBytes()).isZero();
    }

    @Test
    void everyCompositeChargeLeavesAllEarlierFieldsUnchangedWhenALaterFieldRejects() {
        CumulativeRecoveryBudget rootBudget = new CumulativeRecoveryBudget(limits(3, 1000), () -> 0);
        assertThatThrownBy(() -> rootBudget.chargeRoot(true, 4097))
                .isInstanceOf(RecoveryEnvelopeExceededException.class)
                .hasMessageContaining("canonical body bytes");
        assertThat(rootBudget.snapshot().liveRoots()).isZero();
        assertThat(rootBudget.snapshot().predecessorRuns()).isZero();
        assertThat(rootBudget.snapshot().canonicalBodyBytes()).isZero();

        CumulativeRecoveryBudget listBudget = new CumulativeRecoveryBudget(limits(3, 1000), () -> 0);
        assertThatThrownBy(() -> listBudget.chargeListPage(1, 1025))
                .isInstanceOf(RecoveryEnvelopeExceededException.class)
                .hasMessageContaining("LIST key bytes");
        assertThat(listBudget.snapshot().listPages()).isZero();
        assertThat(listBudget.snapshot().listedKeys()).isZero();
        assertThat(listBudget.snapshot().listedKeyBytes()).isZero();

        CumulativeRecoveryBudget decodedBudget = new CumulativeRecoveryBudget(limits(3, 1000), () -> 0);
        assertThatThrownBy(() -> decodedBudget.chargeDecoded(1, 101, 0))
                .isInstanceOf(RecoveryEnvelopeExceededException.class)
                .hasMessageContaining("decoded frames");
        assertThat(decodedBudget.snapshot().decodedContexts()).isZero();
        assertThat(decodedBudget.snapshot().decodedFrames()).isZero();
        assertThat(decodedBudget.snapshot().decodedCommitSets()).isZero();
        assertThatThrownBy(() -> decodedBudget.chargeDecoded(1, 1, 101))
                .isInstanceOf(RecoveryEnvelopeExceededException.class)
                .hasMessageContaining("decoded commit sets");
        assertThat(decodedBudget.snapshot().decodedContexts()).isZero();
        assertThat(decodedBudget.snapshot().decodedFrames()).isZero();
        assertThat(decodedBudget.snapshot().decodedCommitSets()).isZero();
    }

    static RecoveryEnvelopeLimits limits(int listPages, long wallNanos) {
        return new RecoveryEnvelopeLimits(
                4, 3, listPages, 10, 1024, 10, 10, 1, 4096, 100, 100, 100, 100, 1, 2, wallNanos);
    }
}
