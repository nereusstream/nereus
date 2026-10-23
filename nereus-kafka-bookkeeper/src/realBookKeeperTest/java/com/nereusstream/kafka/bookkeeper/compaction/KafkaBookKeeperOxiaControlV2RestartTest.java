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

package com.nereusstream.kafka.bookkeeper.compaction;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.bytes.CanonicalBytes;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Fresh JVM phases separated by an actual restart of the source-locked Oxia server. */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class KafkaBookKeeperOxiaControlV2RestartTest {
    @Test
    void writeBeforeServerRestart() throws Exception {
        var lines = new ArrayList<String>();
        lines.add("KBSD2_KBSD3_RESTART_V1");
        for (var recoveryState : List.of(
                Optional.<KafkaCompactionRecoveryStateV1>empty(),
                Optional.of(KafkaBookKeeperCompactionTestSupportV2.recoveryState(true)))) {
            var input = recoveryState.isPresent()
                    ? KafkaBookKeeperCompactionTestSupportV2.inputWithRecovery(recoveryState.orElseThrow(), 902)
                    : KafkaBookKeeperCompactionTestSupportV2.input(false, 901);
            try (var context = new KafkaBookKeeperOxiaControlV2RealTest.NativeContext(
                    input.layout().task(), KafkaBookKeeperOxiaControlV2RealTest.NativeContext.root())) {
                var descriptor = context.write(input, recoveryState);
                context.publish(input, descriptor, recoveryState);
                var exact = context.onOwner(() -> context.route
                        .read(context.store.selectorKey())
                        .toCompletableFuture()
                        .join()
                        .orElseThrow());
                lines.addAll(List.of(
                        context.root,
                        Base64.getEncoder()
                                .encodeToString(KafkaBookKeeperInventoryCodecV2.encodeTask(context.task)
                                        .toByteArray()),
                        exact.canonicalStoredSha256().toHex(),
                        java.util.HexFormat.of()
                                .formatHex(exact.metadataVersion().value().toByteArray()),
                        descriptor.descriptorSha256().toHex(),
                        recoveryState.map(value -> value.sha256().toHex()).orElse("-")));
            }
        }
        Files.write(checkpoint(), lines);
    }

    @Test
    void readAfterServerRestart() throws Exception {
        var lines = Files.readAllLines(checkpoint());
        assertThat(lines).hasSize(13);
        assertThat(lines.get(0)).isEqualTo("KBSD2_KBSD3_RESTART_V1");
        // Only task/authority identities and hashes cross JVMs; no protocol, output or old source body is supplied.
        for (int row : List.of(1, 7)) {
            var task = KafkaBookKeeperInventoryCodecV2.decodeTask(
                    CanonicalBytes.copyOf(Base64.getDecoder().decode(lines.get(row + 1))));
            try (var context = new KafkaBookKeeperOxiaControlV2RealTest.NativeContext(task, lines.get(row))) {
                var exact = context.onOwner(() -> context.route
                        .read(context.store.selectorKey())
                        .toCompletableFuture()
                        .join()
                        .orElseThrow());
                assertThat(exact.canonicalStoredSha256().toHex()).isEqualTo(lines.get(row + 2));
                assertThat(java.util.HexFormat.of()
                                .formatHex(exact.metadataVersion().value().toByteArray()))
                        .isEqualTo(lines.get(row + 3));
                var view = context.recover();
                assertThat(view.descriptor().descriptorSha256().toHex()).isEqualTo(lines.get(row + 4));
                assertThat(view.recoveryState()
                                .map(value -> value.sha256().toHex())
                                .orElse("-"))
                        .isEqualTo(lines.get(row + 5));
                if (row == 7) {
                    var state = view.recoveryState().orElseThrow();
                    assertThat(view.descriptor().wireVersion()).isEqualTo(3);
                    assertThat(state.encode().length())
                            .isGreaterThan(KafkaSealedBookKeeperDescriptorV2.MAX_ENCODED_BYTES);
                    assertThat(state.components().producerState().producers()).hasSize(20_001);
                    assertThat(state.components()
                                    .producerState()
                                    .producers()
                                    .get(71L)
                                    .coordinatorEpoch())
                            .isEqualTo(3);
                    assertThat(state.components()
                                    .producerState()
                                    .producers()
                                    .get(71L)
                                    .lastMarkerOffset())
                            .isEqualTo(1);
                    assertThat(state.components().transactionState().abortedTransactions())
                            .singleElement()
                            .satisfies(transaction -> assertThat(transaction.markerEndOffsetExclusive())
                                    .isEqualTo(2));
                    assertThat(state.components().leaderEpochIndex().startOffsets())
                            .containsEntry(1, 0L);
                    System.out.println("KBSD3_RECOVERED_AFTER_SERVER_RESTART artifactSha="
                            + state.sha256().toHex() + " bytes="
                            + state.encode().length());
                } else {
                    assertThat(view.descriptor().wireVersion()).isEqualTo(2);
                }
                assertThat(view.lookup(0).orElseThrow().coverage().inclusiveStart())
                        .isEqualTo(1);
                assertThat(view.allowsPredecessorOffset(0)).isFalse();
                assertThat(context.faults.recordCreates.get()).isZero();
                assertThat(context.faults.selectorCas.get()).isZero();
            }
        }
    }

    private static Path checkpoint() {
        return Path.of(System.getProperty("nereus.m5.bkControl.restartCheckpoint"));
    }
}
