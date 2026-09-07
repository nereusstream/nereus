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
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Fresh JVM phases separated by an actual restart of the source-locked Oxia server. */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class KafkaBookKeeperOxiaControlV2RestartTest {
    @Test
    void writeBeforeServerRestart() throws Exception {
        var input = KafkaBookKeeperCompactionTestSupportV2.input(false, 901);
        try (var context = new KafkaBookKeeperOxiaControlV2RealTest.NativeContext(
                input.layout().task(), KafkaBookKeeperOxiaControlV2RealTest.NativeContext.root())) {
            var descriptor = context.write(input);
            context.publish(input, descriptor);
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
                            exact.canonicalStoredSha256().toHex(),
                            java.util.HexFormat.of()
                                    .formatHex(exact.metadataVersion().value().toByteArray()),
                            descriptor.descriptorSha256().toHex()));
        }
    }

    @Test
    void readAfterServerRestart() throws Exception {
        var lines = Files.readAllLines(checkpoint());
        assertThat(lines).hasSize(5);
        // The checkpoint carries only configured task identity and authority hashes, never output or source bodies.
        var task = KafkaBookKeeperInventoryCodecV2.decodeTask(
                CanonicalBytes.copyOf(Base64.getDecoder().decode(lines.get(1))));
        try (var context = new KafkaBookKeeperOxiaControlV2RealTest.NativeContext(task, lines.get(0))) {
            var exact = context.onOwner(() -> context.route
                    .read(context.store.selectorKey())
                    .toCompletableFuture()
                    .join()
                    .orElseThrow());
            assertThat(exact.canonicalStoredSha256().toHex()).isEqualTo(lines.get(2));
            assertThat(java.util.HexFormat.of()
                            .formatHex(exact.metadataVersion().value().toByteArray()))
                    .isEqualTo(lines.get(3));
            var view = context.recover();
            assertThat(view.descriptor().descriptorSha256().toHex()).isEqualTo(lines.get(4));
            assertThat(view.lookup(0).orElseThrow().coverage().inclusiveStart()).isEqualTo(1);
            assertThat(view.allowsPredecessorOffset(0)).isFalse();
            assertThat(context.faults.recordCreates.get()).isZero();
            assertThat(context.faults.selectorCas.get()).isZero();
        }
    }

    private static Path checkpoint() {
        return Path.of(System.getProperty("nereus.m5.bkControl.restartCheckpoint"));
    }
}
