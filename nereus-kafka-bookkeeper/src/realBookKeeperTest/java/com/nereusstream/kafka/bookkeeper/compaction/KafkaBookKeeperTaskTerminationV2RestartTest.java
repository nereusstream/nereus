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
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperOxiaControlV2RealTest.NativeContext;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateSpecV2;
import com.nereusstream.storage.object.retention.M5TaskSelectionCoordinatorV2;
import com.nereusstream.storage.object.retention.M5TaskSelectionDecisionV2;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Two independent JVMs around retained native ZooKeeper, three bookies and Oxia restarts. */
@Timeout(value = 3, unit = TimeUnit.MINUTES)
class KafkaBookKeeperTaskTerminationV2RestartTest {
    @Test
    void writeBeforeServerRestart() throws Exception {
        var input = KafkaBookKeeperNativeCreateV2RealTest.nativeInput(false, 1301);
        var spec = KafkaBookKeeperNativeCreateV2RealTest.spec(input);
        try (var guarded = M5BookKeeperNativeCreateClientV2.connect(
                        uri(), input.layout().task().capability(), spec);
                var context = new NativeContext(input.layout().task(), NativeContext.root(), guarded.newSession())) {
            context.write(input);
            var result = KafkaBookKeeperTaskTerminationV2RealTest.terminate(context, guarded);
            assertThat(result.outcome()).isEqualTo(KafkaBookKeeperTaskTerminationV2.Outcome.TERMINATED_UNPUBLISHED);
            var terminal = result.terminal().orElseThrow();
            assertThat(terminal.physicalCut()).allMatch(part -> part.sealed().isPresent());
            var exact = context.onOwner(() -> context.route
                    .read(context.store.selectorKey())
                    .toCompletableFuture()
                    .join()
                    .orElseThrow());
            Files.write(
                    checkpoint(),
                    List.of(
                            context.root,
                            Base64.getEncoder()
                                    .encodeToString(KafkaBookKeeperInventoryCodecV2.encodeTask(context.task)
                                            .toByteArray()),
                            Base64.getEncoder().encodeToString(spec.encode().toByteArray()),
                            Sha256Digest.hash(terminal.encode()).toHex(),
                            exact.canonicalStoredSha256().toHex(),
                            java.util.HexFormat.of()
                                    .formatHex(exact.metadataVersion().value().toByteArray())));
        }
    }

    @Test
    void readAfterServerRestart() throws Exception {
        var lines = Files.readAllLines(checkpoint());
        assertThat(lines).hasSize(6);
        // Configuration and hashes only. Terminal and native seals must be recovered from their original stores.
        var task = KafkaBookKeeperInventoryCodecV2.decodeTask(
                CanonicalBytes.copyOf(Base64.getDecoder().decode(lines.get(1))));
        var spec = M5BookKeeperNativeCreateSpecV2.decode(
                CanonicalBytes.copyOf(Base64.getDecoder().decode(lines.get(2))));
        assertThat(M5BookKeeperNativeCreateClientV2.discoverInstanceId(uri(), task.capability()))
                .isEqualTo(spec.nativeInstanceId());
        try (var guarded = M5BookKeeperNativeCreateClientV2.connect(uri(), task.capability(), spec);
                var context = new NativeContext(task, lines.get(0), guarded.newSession())) {
            var result = KafkaBookKeeperTaskTerminationV2RealTest.terminate(context, guarded);
            assertThat(result.outcome()).isEqualTo(KafkaBookKeeperTaskTerminationV2.Outcome.TERMINATED_UNPUBLISHED);
            var terminal = result.terminal().orElseThrow();
            assertThat(Sha256Digest.hash(terminal.encode()).toHex()).isEqualTo(lines.get(3));
            for (int ordinal = 0; ordinal < task.parts().size(); ordinal++) {
                var seal = terminal.physicalCut().get(ordinal).sealed().orElseThrow();
                assertThat(guarded.captureExactTarget(seal.handle())
                                .toCompletableFuture()
                                .get(10, TimeUnit.SECONDS)
                                .exactTarget())
                        .contains(seal);
                assertThat(context.session
                                .createReservedRunLedger(
                                        task.configuration(ordinal),
                                        seal.handle().ledgerIdentity())
                                .toCompletableFuture()
                                .get(10, TimeUnit.SECONDS)
                                .exactProof())
                        .isEmpty();
            }
            assertThat(context.session
                            .reserveLedgerIdentity()
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS)
                            .exactProof())
                    .isEmpty();
            var decision = context.onOwner(() -> new M5TaskSelectionCoordinatorV2(context.store, 7, context.binding)
                            .readDecision(task.taskIdSha256()))
                    .orElseThrow();
            assertThat(decision).isEqualTo(terminal.selection());
            assertThat(decision.outcome()).isEqualTo(M5TaskSelectionDecisionV2.Outcome.SELECTION_CANCELLED);
            var exact = context.onOwner(() -> context.route
                    .read(context.store.selectorKey())
                    .toCompletableFuture()
                    .join()
                    .orElseThrow());
            assertThat(exact.canonicalStoredSha256().toHex()).isEqualTo(lines.get(4));
            assertThat(java.util.HexFormat.of()
                            .formatHex(exact.metadataVersion().value().toByteArray()))
                    .isEqualTo(lines.get(5));
            assertThat(context.faults.recordCreates.get()).isZero();
            assertThat(context.faults.selectorCas.get()).isZero();
        }
    }

    private static String uri() {
        return System.getProperty("nereus.bookkeeper.metadataServiceUri");
    }

    private static Path checkpoint() {
        return Path.of(System.getProperty("nereus.m5.bkControl.restartCheckpoint") + ".task-terminal");
    }
}
