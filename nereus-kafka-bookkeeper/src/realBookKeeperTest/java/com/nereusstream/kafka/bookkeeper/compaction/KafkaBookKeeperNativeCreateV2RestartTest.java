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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperOxiaControlV2RealTest.NativeContext;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateSpecV2;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Independent JVMs across owned ZooKeeper, bookie and Oxia container restarts, retaining their exact storage. */
@Timeout(value = 3, unit = TimeUnit.MINUTES)
class KafkaBookKeeperNativeCreateV2RestartTest {
    @Test
    void writeBeforeServerRestart() throws Exception {
        var input = KafkaBookKeeperNativeCreateV2RealTest.nativeInput(false, 910);
        var spec = KafkaBookKeeperNativeCreateV2RealTest.spec(input);
        try (var guarded = M5BookKeeperNativeCreateClientV2.connect(
                        uri(), input.layout().task().capability(), spec);
                var context = new NativeContext(input.layout().task(), NativeContext.root(), guarded.newSession())) {
            var descriptor = context.write(input);
            context.publish(input, descriptor);
            var pending = context.session
                    .reserveLedgerIdentity()
                    .toCompletableFuture()
                    .get(10, TimeUnit.SECONDS)
                    .exactProof()
                    .orElseThrow();
            guarded.fenceCreates().get(10, TimeUnit.SECONDS);
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
                            exact.canonicalStoredSha256().toHex(),
                            java.util.HexFormat.of()
                                    .formatHex(exact.metadataVersion().value().toByteArray()),
                            descriptor.descriptorSha256().toHex(),
                            Long.toString(pending.ledgerId())));
        }
    }

    @Test
    void readAfterServerRestart() throws Exception {
        var lines = Files.readAllLines(checkpoint());
        assertThat(lines).hasSize(7);
        // Identity/configuration and authority hashes only; all output bodies must come from native sealed BK.
        var task = KafkaBookKeeperInventoryCodecV2.decodeTask(
                CanonicalBytes.copyOf(Base64.getDecoder().decode(lines.get(1))));
        var spec = M5BookKeeperNativeCreateSpecV2.decode(
                CanonicalBytes.copyOf(Base64.getDecoder().decode(lines.get(2))));
        assertThat(M5BookKeeperNativeCreateClientV2.discoverInstanceId(uri(), task.capability()))
                .isEqualTo(spec.nativeInstanceId());
        assertThat(task.namespace()).isEqualTo(spec.namespace());
        try (var guarded = M5BookKeeperNativeCreateClientV2.connect(uri(), task.capability(), spec);
                var context = new NativeContext(task, lines.get(0), guarded.newSession())) {
            var pending = new BookKeeperLedgerIdentity(Long.parseLong(lines.get(6)));
            assertThat(context.session
                            .createReservedRunLedger(task.configuration(0), pending)
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS)
                            .exactProof())
                    .isEmpty();
            var bk = (org.apache.bookkeeper.client.BookKeeper) context.bk;
            assertThatThrownBy(() -> bk.getLedgerManager()
                            .readLedgerMetadata(pending.ledgerId())
                            .get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(
                            org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsOnMetadataServerException
                                    .class);
            assertThat(context.session
                            .reserveLedgerIdentity()
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS)
                            .exactProof())
                    .isEmpty();
            var exact = context.onOwner(() -> context.route
                    .read(context.store.selectorKey())
                    .toCompletableFuture()
                    .join()
                    .orElseThrow());
            assertThat(exact.canonicalStoredSha256().toHex()).isEqualTo(lines.get(3));
            assertThat(java.util.HexFormat.of()
                            .formatHex(exact.metadataVersion().value().toByteArray()))
                    .isEqualTo(lines.get(4));
            var view = context.recover();
            assertThat(view.descriptor().descriptorSha256().toHex()).isEqualTo(lines.get(5));
            assertThat(view.lookup(0).orElseThrow().coverage().inclusiveStart()).isEqualTo(1);
            assertThat(view.allowsPredecessorOffset(0)).isFalse();
            assertThat(context.faults.recordCreates.get()).isZero();
            assertThat(context.faults.selectorCas.get()).isZero();
        }
    }

    private static String uri() {
        return System.getProperty("nereus.bookkeeper.metadataServiceUri");
    }

    private static Path checkpoint() {
        return Path.of(System.getProperty("nereus.m5.bkControl.restartCheckpoint") + ".native-create");
    }
}
