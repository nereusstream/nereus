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
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperOxiaControlV2RealTest.NativeContext;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerAppendRequestV1;
import com.nereusstream.storage.bookkeeper.ImmutableRetainedStoragePayload;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome;
import com.nereusstream.storage.object.retention.M5TaskSelectionCoordinatorV2;
import com.nereusstream.storage.object.retention.M5TaskSelectionDecisionV2;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Real locked BK/Oxia cancellation, old-writer drain and selector races; protocol source facts remain fixtures. */
@Timeout(value = 4, unit = TimeUnit.MINUTES)
class KafkaBookKeeperTaskTerminationV2RealTest {
    @Test
    void partialWriterAndUncreatedReservationReachDurableTerminalAndRejectLateNativeWrites() throws Exception {
        var input = KafkaBookKeeperNativeCreateV2RealTest.nativeInput(false, 1101);
        var spec = KafkaBookKeeperNativeCreateV2RealTest.spec(input);
        String root = NativeContext.root();
        KafkaBookKeeperTaskTerminalV2 terminal;
        try (var guarded = connect(input, spec);
                var context = new NativeContext(input.layout().task(), root, guarded.newSession())) {
            initialize(context, input);
            var inventory = new KafkaBookKeeperInventoryV2(context.store, context.owner);
            var first = context.onOwner(() -> inventory.reservePart(context.task, 0, context.session))
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            assertThat(context.onOwner(() -> inventory.createPart(context.task, first, context.session))
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS))
                    .isEqualTo(KafkaBookKeeperInventoryV2.CreateOutcome.CREATED_WRITABLE);
            append(context, first, 0, new byte[] {1, 2, 3}, ProviderMutationOutcomeV1.APPLIED_EXACT);
            var second = context.onOwner(() -> inventory.reservePart(context.task, 1, context.session))
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            var result = terminate(context, guarded);
            assertThat(result.outcome()).isEqualTo(KafkaBookKeeperTaskTerminationV2.Outcome.TERMINATED_UNPUBLISHED);
            terminal = result.terminal().orElseThrow();
            assertThat(terminal.physicalCut().get(0).sealed().orElseThrow().sealedLastEntryId())
                    .isZero();
            assertThat(terminal.physicalCut().get(1).reservedId())
                    .contains(second.handle().ledgerIdentity());
            assertThat(terminal.physicalCut().get(1).sealed()).isEmpty();
            append(context, first, 1, new byte[] {4}, ProviderMutationOutcomeV1.FENCED_OR_CONFLICT);
            assertThat(context.session
                            .createReservedRunLedger(
                                    context.task.configuration(1),
                                    second.handle().ledgerIdentity())
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS)
                            .exactProof())
                    .isEmpty();
            assertThat(context.onOwner(() -> context.m4.readSelector()))
                    .contains(input.plan().sourceCut().predecessorSelector());
        }
        try (var guarded = connect(input, spec);
                var context = new NativeContext(input.layout().task(), root, guarded.newSession())) {
            assertThat(terminate(context, guarded).terminal()).contains(terminal);
            assertThat(context.faults.recordCreates.get()).isZero();
            assertThat(context.faults.selectorCas.get()).isZero();
        }
    }

    @Test
    void cancellingAndArchivingOnAnotherClientDefeatsPausedNativePublicationEvenAfterNextTaskSelects()
            throws Exception {
        var input = KafkaBookKeeperNativeCreateV2RealTest.nativeInput(false, 1102);
        var spec = KafkaBookKeeperNativeCreateV2RealTest.spec(input);
        String root = NativeContext.root();
        try (var guarded = connect(input, spec);
                var first = new NativeContext(input.layout().task(), root, guarded.newSession())) {
            var descriptor = first.write(input);
            first.faults.holdSelector = true;
            var pending = first.onOwner(() -> first.publication()
                            .publish(
                                    input.plan(),
                                    input.semantic(),
                                    descriptor,
                                    first.protections,
                                    () -> new KafkaCompactionPublicationFenceV1().expected(input.plan())))
                    .toCompletableFuture();
            try {
                first.faults.selectorHeld.get(30, TimeUnit.SECONDS);
                try (var otherGuard = connect(input, spec);
                        var cancel = new NativeContext(input.layout().task(), root, otherGuard.newSession())) {
                    assertThat(terminate(cancel, otherGuard).outcome())
                            .isEqualTo(KafkaBookKeeperTaskTerminationV2.Outcome.TERMINATED_UNPUBLISHED);
                    assertThat(cancel.onOwner(() -> cancel.m4.readSelector()))
                            .contains(input.plan().sourceCut().predecessorSelector());
                }
                var nextInput = KafkaBookKeeperNativeCreateV2RealTest.nativeInput(false, 1103);
                var nextSpec = KafkaBookKeeperNativeCreateV2RealTest.spec(nextInput);
                try (var nextGuard = connect(nextInput, nextSpec);
                        var next = new NativeContext(nextInput.layout().task(), root, nextGuard.newSession())) {
                    var nextDescriptor = next.write(nextInput);
                    assertThat(next.publish(nextInput, nextDescriptor)).isEqualTo(PublicationOutcome.APPLIED_EXACT);
                    assertThat(next.recover().descriptor()).isEqualTo(nextDescriptor);
                }
                assertThat(pending).isNotDone();
            } finally {
                Runnable release = first.faults.selectorRelease.getAndSet(null);
                if (release != null) {
                    release.run();
                }
            }
            assertThat(pending.get(30, TimeUnit.SECONDS)).isEqualTo(PublicationOutcome.CANCELLED_STALE);
            assertThat(first.onOwner(() -> new M5TaskSelectionCoordinatorV2(first.store, 7, first.binding)
                                    .readDecision(first.task.taskIdSha256()))
                            .orElseThrow()
                            .outcome())
                    .isEqualTo(M5TaskSelectionDecisionV2.Outcome.SELECTION_CANCELLED);
        }
    }

    @Test
    void alreadySelectedOutputRetainsItsReferenceVetoAndNeverProducesUnpublishedTerminal() throws Exception {
        var input = KafkaBookKeeperNativeCreateV2RealTest.nativeInput(false, 1104);
        var spec = KafkaBookKeeperNativeCreateV2RealTest.spec(input);
        try (var guarded = connect(input, spec);
                var context = new NativeContext(input.layout().task(), NativeContext.root(), guarded.newSession())) {
            var descriptor = context.write(input);
            assertThat(context.publish(input, descriptor)).isEqualTo(PublicationOutcome.APPLIED_EXACT);
            assertThat(terminate(context, guarded).outcome())
                    .isEqualTo(KafkaBookKeeperTaskTerminationV2.Outcome.SELECTED_VETO);
            assertThat(context.onOwner(
                            () -> context.store.get(KafkaBookKeeperTaskTerminalV2.key(context.task.taskIdSha256()))))
                    .isEmpty();
            assertThat(context.recover().descriptor()).isEqualTo(descriptor);
        }
    }

    @Test
    void emptyIndexOnlyCancellationReconcilesLostNativeSelectorArchiveAndTerminalResponses() throws Exception {
        var input = KafkaBookKeeperNativeCreateV2RealTest.nativeInput(true, 1105);
        var spec = KafkaBookKeeperNativeCreateV2RealTest.spec(input);
        try (var guarded = connect(input, spec);
                var context = new NativeContext(input.layout().task(), NativeContext.root(), guarded.newSession())) {
            var descriptor = context.write(input);
            context.faults.loseSelector = true;
            context.faults.loseDecision = true;
            context.faults.loseTerminal = true;
            var result = terminate(context, guarded);
            assertThat(result.outcome()).isEqualTo(KafkaBookKeeperTaskTerminationV2.Outcome.TERMINATED_UNPUBLISHED);
            assertThat(result.terminal().orElseThrow().physicalCut())
                    .allMatch(part -> part.sealed().isPresent());
            assertThat(context.publish(input, descriptor)).isEqualTo(PublicationOutcome.CANCELLED_STALE);
            assertThat(context.faults.selectorCas.get()).isEqualTo(1);
            assertThat(terminate(context, guarded).terminal()).isEqualTo(result.terminal());
        }
    }

    @Test
    void changedNativeRunMetadataKeepsCancellationAndCreateFenceButCannotClaimWriterDrain() throws Exception {
        var input = KafkaBookKeeperNativeCreateV2RealTest.nativeInput(false, 1106);
        var spec = KafkaBookKeeperNativeCreateV2RealTest.spec(input);
        try (var guarded = connect(input, spec);
                var context = new NativeContext(input.layout().task(), NativeContext.root(), guarded.newSession())) {
            initialize(context, input);
            var inventory = new KafkaBookKeeperInventoryV2(context.store, context.owner);
            var part = context.onOwner(() -> inventory.reservePart(context.task, 0, context.session))
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS)
                    .orElseThrow();
            context.onOwner(() -> inventory.createPart(context.task, part, context.session))
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
            var manager = ((org.apache.bookkeeper.client.BookKeeper) context.bk).getLedgerManager();
            long id = part.handle().ledgerIdentity().ledgerId();
            var before = manager.readLedgerMetadata(id).get(10, TimeUnit.SECONDS);
            var custom = new java.util.HashMap<>(before.getValue().getCustomMetadata());
            custom.put("nereus.run-id", new byte[16]);
            var corrupt = org.apache.bookkeeper.client.LedgerMetadataBuilder.from(before.getValue())
                    .withCustomMetadata(custom)
                    .build();
            manager.writeLedgerMetadata(id, corrupt, before.getVersion()).get(10, TimeUnit.SECONDS);
            assertThat(terminate(context, guarded).outcome())
                    .isEqualTo(KafkaBookKeeperTaskTerminationV2.Outcome.RETAIN_UNKNOWN);
            assertThat(context.onOwner(
                            () -> context.store.get(KafkaBookKeeperTaskTerminalV2.key(context.task.taskIdSha256()))))
                    .isEmpty();
            assertThat(context.session
                            .reserveLedgerIdentity()
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS)
                            .exactProof())
                    .isEmpty();
            assertThat(context.onOwner(() -> new M5TaskSelectionCoordinatorV2(context.store, 7, context.binding)
                                    .readDecision(context.task.taskIdSha256()))
                            .orElseThrow()
                            .outcome())
                    .isEqualTo(M5TaskSelectionDecisionV2.Outcome.SELECTION_CANCELLED);
        }
    }

    static void initialize(NativeContext context, KafkaBookKeeperCompactionTestSupportV2.Input input) throws Exception {
        context.protections = context.onOwner(
                () -> KafkaBookKeeperCompactionTestSupportV2.installReadAuthority(context.store, input));
        assertThat(context.onOwner(() -> new KafkaBookKeeperInventoryV2(context.store).register(context.task)))
                .isTrue();
    }

    static KafkaBookKeeperTaskTerminationV2.Result terminate(
            NativeContext context, M5BookKeeperNativeCreateClientV2 guarded) throws Exception {
        return context.onOwner(() -> new KafkaBookKeeperTaskTerminationV2(
                                context.store, 7, context.task, guarded, context.owner)
                        .terminate())
                .toCompletableFuture()
                .get(60, TimeUnit.SECONDS);
    }

    private static M5BookKeeperNativeCreateClientV2 connect(
            KafkaBookKeeperCompactionTestSupportV2.Input input,
            com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateSpecV2 spec)
            throws Exception {
        return M5BookKeeperNativeCreateClientV2.connect(
                System.getProperty("nereus.bookkeeper.metadataServiceUri"),
                input.layout().task().capability(),
                spec);
    }

    private static void append(
            NativeContext context,
            KafkaBookKeeperInventoryV2.Part part,
            long entry,
            byte[] value,
            ProviderMutationOutcomeV1 expected)
            throws Exception {
        var payload = ImmutableRetainedStoragePayload.copyOf(value);
        try {
            assertThat(context.session
                            .appendExplicitEntry(new RunLedgerAppendRequestV1(part.handle(), entry, payload))
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS)
                            .outcome())
                    .isEqualTo(expected);
        } finally {
            payload.release();
        }
    }
}
