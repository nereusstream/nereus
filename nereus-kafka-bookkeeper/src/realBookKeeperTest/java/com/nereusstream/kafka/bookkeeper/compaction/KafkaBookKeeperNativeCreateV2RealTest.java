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
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperOxiaControlV2RealTest.NativeContext;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateSpecV2;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Native BK create fence plus real Oxia inventory/M4 selection. Protocol owner facts remain synthetic. */
@Timeout(value = 3, unit = TimeUnit.MINUTES)
class KafkaBookKeeperNativeCreateV2RealTest {
    @Test
    void fencedCreateScopeStillReadsAndPublishesSealedOutputAcrossFreshClients() throws Exception {
        var input = nativeInput(false, 901);
        var spec = spec(input);
        String root = NativeContext.root();
        KafkaSealedBookKeeperDescriptorV2 descriptor;
        try (var guarded = connect(input, spec);
                var context = new NativeContext(input.layout().task(), root, guarded.newSession())) {
            descriptor = context.write(input);
            assertThat(descriptor.task().namespace()).isEqualTo(guarded.spec().namespace());
            guarded.fenceCreates().get(10, TimeUnit.SECONDS);
            // Closing the create domain is deliberately not a publication-terminal transition.
            assertThat(context.publish(input, descriptor)).isEqualTo(PublicationOutcome.APPLIED_EXACT);
            assertThat(context.recover().descriptor()).isEqualTo(descriptor);
        }
        try (var guarded = connect(input, spec);
                var context = new NativeContext(input.layout().task(), root, guarded.newSession())) {
            var recovered = context.recover();
            assertThat(recovered.descriptor()).isEqualTo(descriptor);
            for (IndexKind kind : IndexKind.values()) {
                assertThat(recovered.index(kind))
                        .isEqualTo(input.semantic().indexes().get(kind.ordinal()));
            }
            assertThat(context.session
                            .reserveLedgerIdentity()
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS)
                            .exactProof())
                    .isEmpty();
            assertThat(context.faults.recordCreates.get()).isZero();
            assertThat(context.faults.selectorCas.get()).isZero();
        }
    }

    @Test
    void inventoriedReservationCannotCreateOrPublishAfterNativeFence() throws Exception {
        var input = nativeInput(false, 902);
        var spec = spec(input);
        try (var guarded = connect(input, spec);
                var context = new NativeContext(input.layout().task(), NativeContext.root(), guarded.newSession())) {
            var inventory = new KafkaBookKeeperInventoryV2(context.store, context.owner);
            var task = input.layout().task();
            assertThat(context.onOwner(() -> inventory.register(task))).isTrue();
            var part = context.onOwner(() -> inventory.reservePart(task, 0, context.session))
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            guarded.fenceCreates().get(10, TimeUnit.SECONDS);
            assertThatThrownBy(() -> context.write(input)).isInstanceOf(java.util.concurrent.ExecutionException.class);
            assertThat(context.onOwner(() -> inventory.readPart(task, 0))).contains(part);
            assertThat(context.faults.partCreates.get()).isEqualTo(1);
            assertThat(context.faults.selectorCas.get()).isZero();
            var nativeClient = (org.apache.bookkeeper.client.BookKeeper) context.bk;
            assertThatThrownBy(() -> nativeClient
                            .getLedgerManager()
                            .readLedgerMetadata(part.handle().ledgerIdentity().ledgerId())
                            .get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(
                            org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsOnMetadataServerException
                                    .class);
        }
    }

    @Test
    void emptyIndexOnlyNativeOutputCannotReappearAfterFenceAndTestDeletion() throws Exception {
        var input = nativeInput(true, 903);
        var spec = spec(input);
        try (var guarded = connect(input, spec);
                var context = new NativeContext(input.layout().task(), NativeContext.root(), guarded.newSession())) {
            var descriptor = context.write(input);
            assertThat(descriptor.task().parts())
                    .allMatch(part -> part.kind() == KafkaBookKeeperInventoryV2.PartKind.INDEX);
            assertThat(context.publish(input, descriptor)).isEqualTo(PublicationOutcome.APPLIED_EXACT);
            guarded.fenceCreates().get(10, TimeUnit.SECONDS);
            var handle = descriptor.sealedParts().get(0).handle();
            // Only a test-owned resource is removed here; no production cleanup authority is exercised.
            context.bk
                    .newDeleteLedgerOp()
                    .withLedgerId(handle.ledgerIdentity().ledgerId())
                    .execute()
                    .get(30, TimeUnit.SECONDS);
            var late = guarded.newSession();
            try {
                assertThat(late.createReservedRunLedger(input.layout().task().configuration(0), handle.ledgerIdentity())
                                .toCompletableFuture()
                                .get(10, TimeUnit.SECONDS)
                                .exactProof())
                        .isEmpty();
            } finally {
                late.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
            }
            assertThatThrownBy(context::recover)
                    .hasRootCauseMessage("selected BK native sealed metadata is missing, changed or unknown");
            assertThat(context.faults.selectorCas.get()).isEqualTo(1);
        }
    }

    static KafkaBookKeeperCompactionTestSupportV2.Input nativeInput(boolean empty, long attempt) throws Exception {
        var synthetic = KafkaBookKeeperCompactionTestSupportV2.input(empty, attempt);
        var capability = synthetic.layout().task().capability();
        var instance = M5BookKeeperNativeCreateClientV2.discoverInstanceId(
                System.getProperty("nereus.bookkeeper.metadataServiceUri"), capability);
        var layout = KafkaBookKeeperCompactionLayoutV2.plan(
                synthetic.plan(),
                synthetic.semantic(),
                M5BookKeeperNativeCreateSpecV2.namespace(instance),
                capability,
                attempt,
                512,
                1024);
        return new KafkaBookKeeperCompactionTestSupportV2.Input(
                synthetic.plan(), synthetic.semantic(), layout, synthetic.capabilityEvidence());
    }

    static M5BookKeeperNativeCreateSpecV2 spec(KafkaBookKeeperCompactionTestSupportV2.Input input) throws Exception {
        var task = input.layout().task();
        var instance = M5BookKeeperNativeCreateClientV2.discoverInstanceId(
                System.getProperty("nereus.bookkeeper.metadataServiceUri"), task.capability());
        assertThat(task.namespace()).isEqualTo(M5BookKeeperNativeCreateSpecV2.namespace(instance));
        return M5BookKeeperNativeCreateSpecV2.of(
                instance,
                task.taskIdSha256(),
                IntStream.range(0, task.parts().size())
                        .mapToObj(task::configuration)
                        .toList());
    }

    private static M5BookKeeperNativeCreateClientV2 connect(
            KafkaBookKeeperCompactionTestSupportV2.Input input, M5BookKeeperNativeCreateSpecV2 spec) throws Exception {
        return M5BookKeeperNativeCreateClientV2.connect(
                System.getProperty("nereus.bookkeeper.metadataServiceUri"),
                input.layout().task().capability(),
                spec);
    }
}
