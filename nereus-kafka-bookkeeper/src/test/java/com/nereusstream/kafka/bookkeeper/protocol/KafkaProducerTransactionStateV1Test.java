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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.kafka.bookkeeper.commit.KafkaBatchDuplicateIdentityV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCommittedProducerStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProducerBatchResultV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProducerSessionStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProtocolAppendPlanV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProtocolBatchDeltaV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaSpeculativeCommitV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionBatchKindV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class KafkaProducerTransactionStateV1Test {
    @Test
    void producerSequenceCoverageMustMatchLogicalOffsetCountIncludingWrap() {
        assertThatThrownBy(() -> batch(1, 0, 0, 2, 1, KafkaTransactionBatchKindV1.NONE, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("coverage");

        KafkaProtocolBatchDeltaV1 wrapped = batch(1, 0, Integer.MAX_VALUE, 0, 2, KafkaTransactionBatchKindV1.NONE, -1);
        assertThat(wrapped.logicalOffsetCount()).isEqualTo(2);
        assertThatThrownBy(() -> KafkaProtocolBatchDeltaV1.nonIdempotent((1L << 31) + 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lastOffsetDelta");
    }

    @Test
    void committedProducerStateReturnsOriginalRangeForNativeDuplicateIdentity() {
        KafkaSpeculativeCommitV1 commit = commit(0, batch(7, 2, 0, 1));
        KafkaCommittedProducerStateV1 state =
                KafkaCommittedProducerStateV1.empty().apply(commit);

        assertThat(state.findDuplicate(new KafkaBatchDuplicateIdentityV1(7, (short) 2, 0, 1)))
                .contains(new KafkaProducerBatchResultV1(new KafkaBatchDuplicateIdentityV1(7, (short) 2, 0, 1), 0, 2));
    }

    @Test
    void recentDuplicateWindowIsHardBounded() {
        KafkaCommittedProducerStateV1 state = KafkaCommittedProducerStateV1.empty();
        for (int sequence = 0; sequence < 7; sequence++) {
            state = state.apply(commit(sequence, batch(7, 2, sequence, sequence)));
        }

        KafkaProducerSessionStateV1 producer = state.producers().get(7L);
        assertThat(producer.recentBatches()).hasSize(KafkaProducerSessionStateV1.MAX_RECENT_BATCHES);
        assertThat(producer.recentBatches().get(0).identity().baseSequence()).isEqualTo(2);
        assertThat(state.findDuplicate(new KafkaBatchDuplicateIdentityV1(7, (short) 2, 0, 0)))
                .isEmpty();
    }

    @Test
    void sequenceGapOrEpochRegressionFailsClosed() {
        KafkaCommittedProducerStateV1 state =
                KafkaCommittedProducerStateV1.empty().apply(commit(0, batch(7, 2, 0, 0)));

        assertThatThrownBy(() -> state.apply(commit(1, batch(7, 2, 2, 2))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("next");
        assertThatThrownBy(() -> state.apply(commit(1, batch(7, 1, 1, 1))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("regresses");
    }

    @Test
    void newerProducerEpochResetsSequenceAndRecentWindow() {
        KafkaCommittedProducerStateV1 state =
                KafkaCommittedProducerStateV1.empty().apply(commit(0, batch(7, 2, 0, 0)));
        state = state.apply(commit(1, batch(7, 3, 0, 0)));

        assertThat(state.producers().get(7L).producerEpoch()).isEqualTo((short) 3);
        assertThat(state.producers().get(7L).recentBatches()).hasSize(1);
    }

    @Test
    void nonIdempotentBatchDoesNotCreateProducerState() {
        KafkaProtocolAppendPlanV1 plan = new KafkaProtocolAppendPlanV1(
                KafkaProtocolStateFixtures.fence(1, 2, 3, 4), List.of(KafkaProtocolBatchDeltaV1.nonIdempotent(1)));
        KafkaSpeculativeCommitV1 commit = KafkaSpeculativeCommitV1.assign(plan, 0, 1);

        assertThat(KafkaCommittedProducerStateV1.empty().apply(commit).producers())
                .isEmpty();
    }

    @Test
    void transactionOpenAndCommitMaintainFirstUnstableAndCompletedRange() {
        KafkaTransactionStateV1 state = KafkaTransactionStateV1.empty()
                .apply(commit(10, transactional(9, 0, KafkaTransactionBatchKindV1.TRANSACTIONAL_DATA, -1)));
        assertThat(state.firstUnstableOffset(10)).hasValue(10);

        state = state.apply(commit(11, transactional(9, 1, KafkaTransactionBatchKindV1.COMMIT_MARKER, 5)));

        assertThat(state.firstUnstableOffset(11)).hasValue(10);
        assertThat(state.firstUnstableOffset(12)).isEmpty();
        assertThat(state.completedTransactions()).singleElement().satisfies(completed -> {
            assertThat(completed.firstOffset()).isEqualTo(10);
            assertThat(completed.markerEndOffsetExclusive()).isEqualTo(12);
            assertThat(completed.aborted()).isFalse();
        });
    }

    @Test
    void abortMarkerBuildsNativeAbortedTransactionIndexAndRequiresAnOpenTransaction() {
        KafkaTransactionStateV1 state = KafkaTransactionStateV1.empty()
                .apply(commit(20, transactional(9, 0, KafkaTransactionBatchKindV1.TRANSACTIONAL_DATA, -1)));
        state = state.apply(commit(21, transactional(9, 1, KafkaTransactionBatchKindV1.ABORT_MARKER, 6)));

        assertThat(state.abortedTransactions()).singleElement().satisfies(aborted -> {
            assertThat(aborted.firstOffset()).isEqualTo(20);
            assertThat(aborted.markerEndOffsetExclusive()).isEqualTo(22);
            assertThat(aborted.coordinatorEpoch()).isEqualTo(6);
        });
        KafkaSpeculativeCommitV1 orphanMarker =
                commit(30, transactional(10, 0, KafkaTransactionBatchKindV1.ABORT_MARKER, 6));
        assertThatThrownBy(() -> KafkaTransactionStateV1.empty().apply(orphanMarker))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no ongoing");
    }

    private static KafkaSpeculativeCommitV1 commit(long startOffset, KafkaProtocolBatchDeltaV1 batch) {
        KafkaProtocolAppendPlanV1 plan =
                new KafkaProtocolAppendPlanV1(KafkaProtocolStateFixtures.fence(1, 2, 3, 4), List.of(batch));
        return KafkaSpeculativeCommitV1.assign(plan, startOffset, startOffset + batch.logicalOffsetCount());
    }

    private static KafkaProtocolBatchDeltaV1 batch(long producerId, int epoch, int baseSequence, int lastSequence) {
        return batch(
                producerId,
                epoch,
                baseSequence,
                lastSequence,
                (long) lastSequence - baseSequence + 1,
                KafkaTransactionBatchKindV1.NONE,
                -1);
    }

    private static KafkaProtocolBatchDeltaV1 transactional(
            long producerId, int sequence, KafkaTransactionBatchKindV1 kind, int coordinatorEpoch) {
        return batch(producerId, 0, sequence, sequence, 1, kind, coordinatorEpoch);
    }

    private static KafkaProtocolBatchDeltaV1 batch(
            long producerId,
            int epoch,
            int baseSequence,
            int lastSequence,
            long offsetCount,
            KafkaTransactionBatchKindV1 kind,
            int coordinatorEpoch) {
        return new KafkaProtocolBatchDeltaV1(
                offsetCount,
                Optional.of(new KafkaBatchDuplicateIdentityV1(producerId, (short) epoch, baseSequence, lastSequence)),
                kind,
                kind == KafkaTransactionBatchKindV1.NONE ? -1 : producerId,
                coordinatorEpoch);
    }
}
