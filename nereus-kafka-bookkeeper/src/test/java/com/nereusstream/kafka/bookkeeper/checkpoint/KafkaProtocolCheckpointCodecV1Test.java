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

package com.nereusstream.kafka.bookkeeper.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCommittedProducerStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaLeaderEpochIndexV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaRunTestFixtures;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.jupiter.api.Test;

class KafkaProtocolCheckpointCodecV1Test {
    @Test
    void roundTripsProducerTransactionAbortedAndLeaderEpochState() {
        KafkaProtocolCheckpointStateV1 expected = KafkaCheckpointTestFixtures.richState();

        KafkaProtocolCheckpointStateV1 decoded = KafkaProtocolCheckpointStateV1.fromNbke2(expected.toNbke2());

        assertThat(decoded).isEqualTo(expected);
        assertThat(decoded.transactionState().abortedTransactions()).hasSize(1);
        assertThat(decoded.producerState()
                        .findDuplicate(new com.nereusstream.kafka.bookkeeper.commit.KafkaBatchDuplicateIdentityV1(
                                71, (short) 0, 1, 1)))
                .isPresent();
    }

    @Test
    void rejectsTrailingBytesUnderStrictEof() {
        KafkaProtocolCheckpointStateV1 state = KafkaCheckpointTestFixtures.richState();
        KafkaProtocolCheckpointSectionsV1 sections = KafkaProtocolCheckpointCodecV1.encode(state);
        byte[] withTrailing = java.util.Arrays.copyOf(
                sections.producerState().toByteArray(), sections.producerState().length() + 1);

        assertThatThrownBy(() -> KafkaProtocolCheckpointCodecV1.decode(
                        state.vector(),
                        new KafkaProtocolCheckpointSectionsV1(
                                CanonicalBytes.copyOf(withTrailing),
                                sections.transactionIndex(),
                                sections.leaderEpochIndex())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("trailing");
    }

    @Test
    void rejectsTruncationAndMagicSubstitution() {
        KafkaProtocolCheckpointStateV1 state = KafkaCheckpointTestFixtures.richState();
        KafkaProtocolCheckpointSectionsV1 sections = KafkaProtocolCheckpointCodecV1.encode(state);
        byte[] truncated = java.util.Arrays.copyOf(
                sections.transactionIndex().toByteArray(),
                sections.transactionIndex().length() - 1);
        byte[] wrongMagic = sections.leaderEpochIndex().toByteArray();
        wrongMagic[0] ^= 1;

        assertThatThrownBy(() -> KafkaProtocolCheckpointCodecV1.decode(
                        state.vector(),
                        new KafkaProtocolCheckpointSectionsV1(
                                sections.producerState(),
                                CanonicalBytes.copyOf(truncated),
                                sections.leaderEpochIndex())))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaProtocolCheckpointCodecV1.decode(
                        state.vector(),
                        new KafkaProtocolCheckpointSectionsV1(
                                sections.producerState(),
                                sections.transactionIndex(),
                                CanonicalBytes.copyOf(wrongMagic))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("magic");
    }

    @Test
    void rejectsCountBombBeforeAllocatingRows() {
        byte[] bomb = ByteBuffer.allocate(8)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(0x4b_4c_43_31)
                .putInt(65_537)
                .array();
        KafkaProtocolCheckpointStateV1 state = KafkaCheckpointTestFixtures.richState();
        KafkaProtocolCheckpointSectionsV1 sections = KafkaProtocolCheckpointCodecV1.encode(state);

        assertThatThrownBy(() -> KafkaProtocolCheckpointCodecV1.decode(
                        state.vector(),
                        new KafkaProtocolCheckpointSectionsV1(
                                sections.producerState(), sections.transactionIndex(), CanonicalBytes.copyOf(bomb))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("count");
    }

    @Test
    void vectorUsesMinimumCompatibleBoundaryAndDetectsRegression() {
        var binding = KafkaRunTestFixtures.binding(6, 11, 5);
        KafkaRecoveryCheckpointVectorV1 vector = new KafkaRecoveryCheckpointVectorV1(binding, 105, 104, 103, 102);
        KafkaRecoveryCheckpointVectorV1 previous = new KafkaRecoveryCheckpointVectorV1(binding, 100, 100, 100, 100);

        assertThat(vector.recoveryCoveredThrough()).isEqualTo(102);
        assertThat(vector.isAlignedCompoundCheckpoint()).isFalse();
        assertThat(vector.doesNotRegress(previous)).isTrue();
        assertThat(previous.doesNotRegress(vector)).isFalse();
    }

    @Test
    void stateRejectsAComponentThatEscapesItsCoverage() {
        KafkaProtocolCheckpointStateV1 rich = KafkaCheckpointTestFixtures.richState();
        KafkaRecoveryCheckpointVectorV1 tooShort =
                new KafkaRecoveryCheckpointVectorV1(rich.vector().runBinding(), 101, 101, 101, 101);

        assertThatThrownBy(() -> new KafkaProtocolCheckpointStateV1(
                        tooShort,
                        rich.producerState(),
                        KafkaTransactionStateV1.empty(),
                        KafkaLeaderEpochIndexV1.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("producer");
        assertThat(new KafkaProtocolCheckpointStateV1(
                                tooShort,
                                KafkaCommittedProducerStateV1.empty(),
                                KafkaTransactionStateV1.empty(),
                                KafkaLeaderEpochIndexV1.empty())
                        .vector()
                        .recoveryCoveredThrough())
                .isEqualTo(101);
    }
}
