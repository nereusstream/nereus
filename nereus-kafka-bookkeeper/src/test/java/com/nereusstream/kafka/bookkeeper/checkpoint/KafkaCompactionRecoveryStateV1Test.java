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
import com.nereusstream.kafka.bookkeeper.commit.KafkaBatchDuplicateIdentityV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentCommitCoordinatorV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecoveryStateCodecV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecoveryStateV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaRunTestFixtures;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class KafkaCompactionRecoveryStateV1Test {
    @Test
    void fullRecoveryStateKeepsDuplicatesMarkersTransactionsAndEpochsWithoutPhysicalLocators() {
        var original = KafkaCheckpointTestFixtures.richState();
        var input = new KafkaProtocolCheckpointStateV1(
                original.vector(),
                original.producerState(),
                original.transactionState(),
                original.leaderEpochIndex(),
                List.of(new KafkaCheckpointReadIndexV1(
                        original.vector().runBinding().runId(),
                        List.of(new KafkaCheckpointReadIndexV1.Row(100, 102, 7, 2, 200)))));
        var expected = state(input);
        var recovered = KafkaCompactionRecoveryStateCodecV1.decode(expected.encode());
        assertThat(recovered).isEqualTo(expected);
        var output = recovered.checkpointFor(KafkaRunTestFixtures.binding(7, 11, 5));
        assertThat(output.readIndexes()).isEmpty();
        assertThat(output.components()).isEqualTo(input.components());
        assertThat(output.vector().runBinding()).isNotEqualTo(input.vector().runBinding());
        assertThat(output.producerState().findDuplicate(new KafkaBatchDuplicateIdentityV1(71, (short) 0, 0, 0)))
                .hasValueSatisfying(batch -> {
                    assertThat(batch.startOffset()).isEqualTo(100);
                    assertThat(batch.endOffsetExclusive()).isEqualTo(101);
                });
        assertThat(output.producerState().producers().get(71L).coordinatorEpoch())
                .isEqualTo(3);
        assertThat(output.producerState().producers().get(71L).lastMarkerOffset())
                .isEqualTo(101);
        assertThat(output.transactionState().completedTransactions())
                .singleElement()
                .satisfies(transaction -> {
                    assertThat(transaction.aborted()).isTrue();
                    assertThat(transaction.markerEndOffsetExclusive()).isEqualTo(102);
                });
        assertThat(output.leaderEpochIndex()).isEqualTo(input.leaderEpochIndex());
        assertThatThrownBy(() ->
                        KafkaProtocolCheckpointCodecV1.decodeComponents(KafkaProtocolCheckpointCodecV1.encode(input)))
                .hasMessageContaining("physical source locators");
    }

    @Test
    void recoveryWireRejectsTruncationForeignVersionTrailingAndOversizedBytes() {
        var expected = state(KafkaCheckpointTestFixtures.richState());
        byte[] bytes = expected.encode().toByteArray();
        for (int length : List.of(0, 5, bytes.length - 1, bytes.length + 1)) {
            assertThatThrownBy(() -> KafkaCompactionRecoveryStateCodecV1.decode(
                            CanonicalBytes.copyOf(Arrays.copyOf(bytes, length))))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        bytes[5] = 2;
        assertThatThrownBy(() -> KafkaCompactionRecoveryStateCodecV1.decode(CanonicalBytes.copyOf(bytes)))
                .hasMessageContaining("foreign magic or version");
        assertThatThrownBy(() -> KafkaCompactionRecoveryStateCodecV1.decode(
                        CanonicalBytes.copyOf(new byte[KafkaCompactionRecoveryStateCodecV1.MAX_ENCODED_BYTES + 1])))
                .hasMessageContaining("encoded bound");
    }

    @Test
    void recoveryStateRejectsEscapedCoverageAndForeignOrOlderOutputFence() {
        var expected = state(KafkaCheckpointTestFixtures.richState());
        assertThatThrownBy(() -> new KafkaCompactionRecoveryStateV1(
                        expected.sourceFence(), 101, expected.sourceCheckpointSha256(), expected.components()))
                .hasMessageContaining("escapes");
        for (var output : List.of(KafkaRunTestFixtures.binding(7, 10, 5), KafkaRunTestFixtures.binding(7, 11, 4))) {
            assertThatThrownBy(() -> expected.checkpointFor(output)).hasMessageContaining("foreign or older");
        }
        var original = KafkaRunTestFixtures.binding(7, 11, 5);
        var foreign = new com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1(
                original.bindingId(),
                original.topicIncarnation(),
                original.partitionId() + 1,
                original.storageEpochId(),
                original.creatorOwnerEpoch(),
                original.kafkaLeaderEpoch(),
                original.providerScopeId(),
                original.runId());
        assertThatThrownBy(() -> expected.checkpointFor(foreign)).hasMessageContaining("foreign or older");
    }

    private static KafkaCompactionRecoveryStateV1 state(KafkaProtocolCheckpointStateV1 checkpoint) {
        return new KafkaCompactionRecoveryStateV1(
                KafkaCheckpointTestFixtures.fence(),
                checkpoint.vector().recoveryCoveredThrough(),
                KafkaCoherentCommitCoordinatorV1.recoveryCheckpointContentDigest(checkpoint),
                checkpoint.components());
    }
}
