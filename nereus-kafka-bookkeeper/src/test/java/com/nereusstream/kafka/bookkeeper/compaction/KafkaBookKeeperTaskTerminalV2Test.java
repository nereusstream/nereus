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
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.BookKeeperDeleteTargetV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateSpecV2;
import com.nereusstream.storage.object.retention.M5TaskSelectionDecisionV2;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class KafkaBookKeeperTaskTerminalV2Test {
    @Test
    void completePhysicalCutRoundTripsMissingReservedAndSealedNativeStates() {
        var terminal = terminal();
        assertThat(terminal.physicalCut().size()).isGreaterThan(2);
        assertThat(KafkaBookKeeperTaskTerminalV2.decode(terminal.encode())).isEqualTo(terminal);
        assertThat(terminal.physicalCut().get(0).sealed().orElseThrow().sealedLastEntryId())
                .isEqualTo(2);
        assertThat(terminal.physicalCut().get(1).reservedId()).contains(new BookKeeperLedgerIdentity(102));
        assertThat(terminal.physicalCut().get(2).reservedId()).isEmpty();
    }

    @Test
    void truncatedNoncanonicalAndOversizedTerminalsCannotBecomeValidCuts() {
        var bytes = terminal().encode().toByteArray();
        for (int length : List.of(0, 1, 7, 8, 12, 100, bytes.length - 1)) {
            var truncated = CanonicalBytes.copyOf(Arrays.copyOf(bytes, length));
            assertThatThrownBy(() -> KafkaBookKeeperTaskTerminalV2.decode(truncated))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> KafkaBookKeeperTaskTerminalV2.decode(
                        CanonicalBytes.copyOf(Arrays.copyOf(bytes, bytes.length + 1))))
                .hasMessageContaining("canonical");
        var version = bytes.clone();
        version[7] = 3;
        assertThatThrownBy(() -> KafkaBookKeeperTaskTerminalV2.decode(CanonicalBytes.copyOf(version)))
                .hasMessageContaining("version");
        assertThatThrownBy(() -> KafkaBookKeeperTaskTerminalV2.decode(
                        CanonicalBytes.copyOf(new byte[KafkaBookKeeperTaskTerminalV2.MAX_BYTES + 1])))
                .hasMessageContaining("length");
    }

    @Test
    void foreignNativeScopeDuplicateIdsAndIncompleteCutsFailClosed() {
        var terminal = terminal();
        var scope = terminal.nativeCreateScope();
        var wrongScope =
                M5BookKeeperNativeCreateSpecV2.of(UUID.randomUUID().toString(), scope.taskId(), scope.configurations());
        assertThatThrownBy(() -> new KafkaBookKeeperTaskTerminalV2(
                        terminal.task(), wrongScope, terminal.selection(), terminal.physicalCut()))
                .hasMessageContaining("native create scope");
        var duplicate = new ArrayList<>(terminal.physicalCut());
        duplicate.set(
                1,
                new KafkaBookKeeperTaskTerminalV2.DrainedPart(duplicate.get(0).reservedId(), Optional.empty()));
        assertThatThrownBy(() ->
                        new KafkaBookKeeperTaskTerminalV2(terminal.task(), scope, terminal.selection(), duplicate))
                .hasMessageContaining("repeats");
        assertThatThrownBy(() ->
                        new KafkaBookKeeperTaskTerminalV2(terminal.task(), scope, terminal.selection(), List.of()))
                .hasMessageContaining("complete bounded");
        var other = new M5TaskSelectionDecisionV2(
                terminal.selection().binding(),
                KafkaBookKeeperCompactionTestSupportV2.digest("other-task"),
                terminal.selection().outcome(),
                Optional.empty(),
                terminal.selection().predecessorSelector(),
                terminal.selection().successorSelector());
        assertThatThrownBy(
                        () -> new KafkaBookKeeperTaskTerminalV2(terminal.task(), scope, other, terminal.physicalCut()))
                .hasMessageContaining("cancelled task");
    }

    @Test
    void typedRouteRequiresExactRegisteredTaskAndCancelledSelectionArchive() {
        var terminal = terminal();
        var raw = new KafkaBookKeeperCompactionTestSupportV2.Store();
        var task = terminal.task();
        var store = new KafkaBookKeeperControlMetadataStoreV2(
                raw,
                raw,
                7,
                terminal.selection().binding(),
                task.namespace(),
                task.capability().providerScopeId());
        String key = KafkaBookKeeperTaskTerminalV2.key(task.taskIdSha256());
        assertThatThrownBy(() -> store.putIfAbsent(key, terminal.encode())).hasMessageContaining("immutable task");
        assertThat(new KafkaBookKeeperInventoryV2(store).register(task)).isTrue();
        assertThatThrownBy(() -> store.putIfAbsent(key, terminal.encode())).hasMessageContaining("selection");
        raw.putIfAbsent(terminal.selection().key(), terminal.selection().encode());
        store.putIfAbsent(key, terminal.encode());
        assertThat(KafkaBookKeeperTaskTerminalV2.decode(store.get(key).orElseThrow()))
                .isEqualTo(terminal);
    }

    private static KafkaBookKeeperTaskTerminalV2 terminal() {
        var input = KafkaBookKeeperCompactionTestSupportV2.input(false, 1201);
        String instance = "09a0c951-2b5a-4c97-b8d1-715d01000001";
        var task = KafkaBookKeeperCompactionLayoutV2.plan(
                        input.plan(),
                        input.semantic(),
                        M5BookKeeperNativeCreateSpecV2.namespace(instance),
                        input.layout().task().capability(),
                        1201,
                        512,
                        1024)
                .task();
        var scope = M5BookKeeperNativeCreateSpecV2.of(
                instance,
                task.taskIdSha256(),
                IntStream.range(0, task.parts().size())
                        .mapToObj(task::configuration)
                        .toList());
        var selector = input.plan().sourceCut().predecessorSelector();
        var decision = M5TaskSelectionDecisionV2.of(
                task.taskIdSha256(), M5TaskSelectionDecisionV2.Outcome.SELECTION_CANCELLED, selector, selector);
        var cut = new ArrayList<KafkaBookKeeperTaskTerminalV2.DrainedPart>();
        var run = task.configuration(0);
        var seal = new BookKeeperDeleteTargetV1(
                new RunLedgerHandleV1(
                        run.providerScopeId(),
                        run.runId(),
                        new BookKeeperLedgerIdentity(101),
                        run.configurationDigest()),
                2,
                3,
                run.ensembleSize(),
                run.writeQuorumSize(),
                run.ackQuorumSize(),
                run.digestType(),
                task.capability().credentialIdentityVersion(),
                Sha256Digest.hash(CanonicalBytes.empty()),
                3,
                7,
                KafkaBookKeeperCompactionTestSupportV2.digest("native-seal"));
        cut.add(new KafkaBookKeeperTaskTerminalV2.DrainedPart(
                Optional.of(seal.handle().ledgerIdentity()), Optional.of(seal)));
        cut.add(new KafkaBookKeeperTaskTerminalV2.DrainedPart(
                Optional.of(new BookKeeperLedgerIdentity(102)), Optional.empty()));
        while (cut.size() < task.parts().size()) {
            cut.add(new KafkaBookKeeperTaskTerminalV2.DrainedPart(Optional.empty(), Optional.empty()));
        }
        return new KafkaBookKeeperTaskTerminalV2(task, scope, decision, cut);
    }
}
