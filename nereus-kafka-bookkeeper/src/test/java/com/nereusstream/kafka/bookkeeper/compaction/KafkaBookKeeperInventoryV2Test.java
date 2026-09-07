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
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionTestSupportV2.Store;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.CreateOutcome;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.Part;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.PartKind;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerOpenOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerOpenResultV1;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class KafkaBookKeeperInventoryV2Test {
    @Test
    void layoutSegmentsOnlyRetainedBatchesAndAllEightIndexesWithinNativeBounds() {
        var layout = KafkaBookKeeperCompactionTestSupportV2.layout(false, 0);
        assertThat(layout).isEqualTo(KafkaBookKeeperCompactionTestSupportV2.layout(false, 0));
        assertThat(layout.task().taskIdSha256())
                .isNotEqualTo(KafkaBookKeeperCompactionTestSupportV2.layout(false, 1)
                        .task()
                        .taskIdSha256());
        assertThat(layout.parts().size()).isGreaterThan(2);
        var artifacts = artifacts(layout);
        assertThat(artifacts).hasSize(9);
        var retained = KafkaRecordBatchCodecV1.parse(artifacts.get("DATA:0"));
        assertThat(retained.records())
                .extracting(KafkaCompactionRecordsV1.RecordValue::offset)
                .containsExactly(1L);
        assertThat(retained.records().get(0).value().orElseThrow().length()).isEqualTo(1024);
        for (int index = 0; index < 8; index++) {
            assertThat(KafkaCompactionIndexV1.decode(artifacts.get("INDEX:" + index))
                            .kind()
                            .ordinal())
                    .isEqualTo(index);
        }
    }

    @Test
    void emptyOutputHasNoDataLedgerButKeepsEightDecodableIndexesAndGap() {
        var layout = KafkaBookKeeperCompactionTestSupportV2.layout(true, 0);
        assertThat(layout.parts()).allMatch(part -> part.kind() == PartKind.INDEX);
        var artifacts = artifacts(layout);
        assertThat(artifacts).hasSize(8);
        var coverage = KafkaCompactionIndexV1.decode(artifacts.get("INDEX:7"));
        assertThat(coverage.rows()).singleElement().satisfies(row -> assertThat(
                        row.flags() & KafkaCompactionIndexV1.FLAG_GAP)
                .isNotZero());
    }

    @Test
    void taskAndPartCodecRejectTrailingTruncatedForeignVersionAndOversizedInput() {
        var fixture = new Fixture();
        assertThat(fixture.inventory.register(fixture.task)).isTrue();
        var part = fixture.reserve();
        var taskBytes = KafkaBookKeeperInventoryCodecV2.encodeTask(fixture.task);
        assertThat(KafkaBookKeeperInventoryCodecV2.decodeTask(taskBytes)).isEqualTo(fixture.task);
        var partBytes = KafkaBookKeeperInventoryCodecV2.encodePart(part);
        assertThat(KafkaBookKeeperInventoryCodecV2.decodePart(partBytes)).isEqualTo(part);
        for (int size : List.of(0, 6, partBytes.length() - 1, partBytes.length() + 1)) {
            var malformed = CanonicalBytes.copyOf(Arrays.copyOf(partBytes.toByteArray(), size));
            assertThatThrownBy(() -> KafkaBookKeeperInventoryCodecV2.decodePart(malformed))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        byte[] version = taskBytes.toByteArray();
        version[5] = 1;
        assertThatThrownBy(() -> KafkaBookKeeperInventoryCodecV2.decodeTask(CanonicalBytes.copyOf(version)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaBookKeeperInventoryCodecV2.decodeTask(
                        CanonicalBytes.copyOf(new byte[KafkaBookKeeperInventoryV2.MAX_TASK_BYTES + 1])))
                .isInstanceOf(IllegalArgumentException.class);
        var entry = KafkaBookKeeperCompactionTestSupportV2.layout(false, 0)
                .parts()
                .get(0)
                .entries()
                .get(0);
        assertThatThrownBy(() -> KafkaBookKeeperCompactionLayoutV2.decodeChunk(entry, entry.length() - 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaBookKeeperCompactionLayoutV2.decodeChunk(
                        CanonicalBytes.copyOf(Arrays.copyOf(entry.toByteArray(), entry.length() - 1)), 512))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void unknownTaskOrAllocationNeverDispatchesLedgerCreation() {
        var fixture = new Fixture();
        fixture.store.dropNextPut = true;
        assertThat(fixture.inventory.register(fixture.task)).isFalse();
        assertThatThrownBy(fixture::reserve).isInstanceOf(IllegalStateException.class);
        assertThat(fixture.allocations).isZero();
        assertThat(fixture.inventory.register(fixture.task)).isTrue();
        fixture.store.dropNextPut = true;
        assertThat(fixture.inventory
                        .reservePart(fixture.task, 0, fixture.session)
                        .toCompletableFuture()
                        .join())
                .isEmpty();
        assertThat(fixture.creates).isEmpty();
        assertThat(fixture.inventory.readPart(fixture.task, 0)).isEmpty();
        assertThat(fixture.allocations).isEqualTo(1);
    }

    @Test
    void lostInventoryResponseAndRestartRetainTheSameIdAndCreateOnlyAfterExactReread() {
        var fixture = new Fixture();
        fixture.inventory.register(fixture.task);
        fixture.store.loseNextPutResponse = true;
        Part part = fixture.reserve();
        var restarted = new KafkaBookKeeperInventoryV2(fixture.store);
        assertThat(restarted
                        .reservePart(fixture.task, 0, fixture.session)
                        .toCompletableFuture()
                        .join())
                .contains(part);
        assertThat(fixture.allocations).isEqualTo(1);
        assertThat(restarted
                        .createPart(fixture.task, part, fixture.session)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(CreateOutcome.CREATED_WRITABLE);
        assertThat(fixture.creates).containsExactly(part.handle().ledgerIdentity());
        assertThat(fixture.store.operations.get(fixture.store.operations.size() - 1))
                .isEqualTo("read:" + KafkaBookKeeperInventoryV2.partKey(fixture.task.taskIdSha256(), 0));
    }

    @Test
    void concurrentNativeReservationsAdoptTheWinnerAndBurnOnlyTheUnusedId() {
        var fixture = new Fixture();
        fixture.inventory.register(fixture.task);
        fixture.delayAllocation = true;
        var first = fixture.inventory.reservePart(fixture.task, 0, fixture.session);
        var second = fixture.inventory.reservePart(fixture.task, 0, fixture.session);
        fixture.pending.get(1).complete(ProviderMutationResultV1.appliedExact(new BookKeeperLedgerIdentity(82)));
        fixture.pending.get(0).complete(ProviderMutationResultV1.appliedExact(new BookKeeperLedgerIdentity(81)));
        assertThat(first.toCompletableFuture().join())
                .isEqualTo(second.toCompletableFuture().join());
        assertThat(first.toCompletableFuture().join().orElseThrow().resource().ledgerId())
                .isEqualTo(82);
        assertThat(fixture.creates).isEmpty();
    }

    @Test
    void lostCreateResponseRequiresRecoveryAndForeignRunNeverBecomesWritable() {
        var fixture = new Fixture();
        fixture.inventory.register(fixture.task);
        Part part = fixture.reserve();
        fixture.unknownCreate = true;
        fixture.open = RunLedgerOpenResultV1.openedExact(part.handle());
        assertThat(fixture.inventory
                        .createPart(fixture.task, part, fixture.session)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(CreateOutcome.EXISTING_EXACT_REQUIRES_RECOVERY);
        fixture.open = RunLedgerOpenResultV1.withoutHandle(RunLedgerOpenOutcomeV1.CONFIGURATION_MISMATCH);
        assertThat(fixture.inventory
                        .createPart(fixture.task, part, fixture.session)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(CreateOutcome.CONFLICT);
        assertThat(fixture.allocations).isEqualTo(1);
        assertThat(fixture.creates)
                .containsExactly(part.handle().ledgerIdentity(), part.handle().ledgerIdentity());
    }

    @Test
    void corruptedInventoryCannotDispatchAProviderOperation() {
        var fixture = new Fixture();
        fixture.inventory.register(fixture.task);
        Part part = fixture.reserve();
        var foreign = new Part(part.taskIdSha256(), 1, part.resource(), part.handle());
        fixture.store.values.put(
                KafkaBookKeeperInventoryV2.partKey(fixture.task.taskIdSha256(), 0),
                KafkaBookKeeperInventoryCodecV2.encodePart(foreign));
        assertThatThrownBy(() -> fixture.inventory.createPart(fixture.task, part, fixture.session))
                .isInstanceOf(IllegalStateException.class);
        assertThat(fixture.creates).isEmpty();
        assertThatThrownBy(fixture::reserve).isInstanceOf(IllegalStateException.class);
        assertThat(fixture.allocations).isEqualTo(1);
    }

    private static Map<String, CanonicalBytes> artifacts(KafkaBookKeeperCompactionLayoutV2.Layout layout) {
        Map<String, ByteArrayOutputStream> bodies = new LinkedHashMap<>();
        Map<String, Integer> counts = new LinkedHashMap<>();
        Map<String, KafkaBookKeeperCompactionLayoutV2.Chunk> last = new LinkedHashMap<>();
        for (var part : layout.parts()) {
            assertThat(part.plan().length()).isLessThanOrEqualTo(1024);
            for (var entry : part.entries()) {
                var chunk = KafkaBookKeeperCompactionLayoutV2.decodeChunk(entry, 512);
                assertThat(KafkaBookKeeperCompactionLayoutV2.encodeChunk(chunk)).isEqualTo(entry);
                String key = chunk.kind() + ":" + chunk.artifactOrdinal();
                int count = counts.getOrDefault(key, 0);
                assertThat(chunk.chunkOrdinal()).isEqualTo(count);
                counts.put(key, count + 1);
                bodies.computeIfAbsent(key, ignored -> new ByteArrayOutputStream())
                        .writeBytes(chunk.body().toByteArray());
                last.put(key, chunk);
            }
        }
        Map<String, CanonicalBytes> result = new LinkedHashMap<>();
        bodies.forEach((key, value) -> {
            CanonicalBytes body = CanonicalBytes.copyOf(value.toByteArray());
            assertThat(body.length()).isEqualTo(last.get(key).artifactLength());
            assertThat(Sha256Digest.hash(body)).isEqualTo(last.get(key).artifactSha256());
            assertThat(counts.get(key)).isEqualTo(last.get(key).chunkCount());
            result.put(key, body);
        });
        return result;
    }

    private static final class Fixture {
        final KafkaBookKeeperInventoryV2.Task task =
                KafkaBookKeeperCompactionTestSupportV2.layout(false, 0).task();
        final Store store = new Store();
        final KafkaBookKeeperInventoryV2 inventory = new KafkaBookKeeperInventoryV2(store);
        final List<BookKeeperLedgerIdentity> creates = new ArrayList<>();
        final List<CompletableFuture<ProviderMutationResultV1<BookKeeperLedgerIdentity>>> pending = new ArrayList<>();
        int allocations;
        boolean delayAllocation;
        boolean unknownCreate;
        RunLedgerOpenResultV1 open;
        final BookKeeperCellSession session = (BookKeeperCellSession) Proxy.newProxyInstance(
                BookKeeperCellSession.class.getClassLoader(),
                new Class<?>[] {BookKeeperCellSession.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "capabilitySnapshot" -> task.capability();
                    case "reserveLedgerIdentity" -> allocate();
                    case "createReservedRunLedger" -> {
                        var configuration = (RunLedgerConfigurationV1) arguments[0];
                        var id = (BookKeeperLedgerIdentity) arguments[1];
                        creates.add(id);
                        yield CompletableFuture.completedFuture(
                                unknownCreate
                                        ? ProviderMutationResultV1.outcomeUnknown()
                                        : ProviderMutationResultV1.appliedExact(new RunLedgerHandleV1(
                                                configuration.providerScopeId(),
                                                configuration.runId(),
                                                id,
                                                configuration.configurationDigest())));
                    }
                    case "openRunLedger" -> CompletableFuture.completedFuture(open);
                    default -> throw new AssertionError("unexpected provider operation: " + method.getName());
                });

        private CompletionStage<ProviderMutationResultV1<BookKeeperLedgerIdentity>> allocate() {
            allocations++;
            if (!delayAllocation) {
                return CompletableFuture.completedFuture(
                        ProviderMutationResultV1.appliedExact(new BookKeeperLedgerIdentity(allocations)));
            }
            var result = new CompletableFuture<ProviderMutationResultV1<BookKeeperLedgerIdentity>>();
            pending.add(result);
            return result;
        }

        Part reserve() {
            return inventory
                    .reservePart(task, 0, session)
                    .toCompletableFuture()
                    .join()
                    .orElseThrow();
        }
    }
}
