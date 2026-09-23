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
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionWriterV2.VerifiedPart;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.ExactLedgerEntryV1;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerOpenResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadResultV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.BookKeeperDeleteTargetV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.CaptureOutcome;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.CaptureResult;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;
import com.nereusstream.storage.object.gc.SyntheticDeleteAuthorityFixturesV2;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1;
import com.nereusstream.storage.object.retention.M5TaskSelectionCoordinatorV2;
import com.nereusstream.storage.object.retention.M5TaskSelectionDecisionV2;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class KafkaSealedBookKeeperDescriptorV2Test {
    @Test
    void strictDescriptorRoundTripRejectsTrailingTruncatedForeignVersionAndOversizedBytes() {
        for (boolean empty : List.of(false, true)) {
            var fixture = new Fixture(empty);
            var bytes = fixture.descriptor.encode();
            assertThat(KafkaSealedBookKeeperDescriptorCodecV2.decode(bytes)).isEqualTo(fixture.descriptor);
            for (int length : List.of(0, 6, bytes.length() - 1, bytes.length() + 1)) {
                var bad = CanonicalBytes.copyOf(Arrays.copyOf(bytes.toByteArray(), length));
                assertThatThrownBy(() -> KafkaSealedBookKeeperDescriptorCodecV2.decode(bad))
                        .isInstanceOf(IllegalArgumentException.class);
            }
            byte[] old = bytes.toByteArray();
            old[5] = 1;
            assertThatThrownBy(() -> KafkaSealedBookKeeperDescriptorCodecV2.decode(CanonicalBytes.copyOf(old)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> KafkaSealedBookKeeperDescriptorCodecV2.decode(
                        CanonicalBytes.copyOf(new byte[KafkaSealedBookKeeperDescriptorV2.MAX_ENCODED_BYTES + 1])))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void recoveryUsesOnlyDescriptorAndLedgerBytesAndPreservesSuccessorTimestampAndSuppression() {
        var fixture = new Fixture(false);
        var recovered = fixture.reader(1_000_000)
                .recover(KafkaSealedBookKeeperDescriptorCodecV2.decode(fixture.descriptor.encode()))
                .toCompletableFuture()
                .join();
        assertThat(recovered.lookup(0).orElseThrow().coverage().inclusiveStart())
                .isEqualTo(1);
        assertThat(recovered.listOffset(2)).hasValue(1);
        assertThat(recovered.allowsPredecessorOffset(0)).isFalse();
        assertThat(recovered.allowsPredecessorOffset(1)).isTrue();
        assertThat(recovered.gaps()).containsExactly(new KafkaCompactionRecordsV1.Gap(0, 1));
        assertThat(KafkaRecordBatchCodecV1.parse(recovered.readBatch(0)).records())
                .extracting(KafkaCompactionRecordsV1.RecordValue::offset)
                .containsExactly(1L);
        for (IndexKind kind : IndexKind.values()) {
            assertThat(recovered.index(kind))
                    .isEqualTo(fixture.input.semantic().indexes().get(kind.ordinal()));
        }
    }

    @Test
    void emptyDescriptorRecoversCompleteGapAndIndexesWithoutADataLedger() {
        var fixture = new Fixture(true);
        var view = fixture.reader(1_000_000)
                .recover(fixture.descriptor)
                .toCompletableFuture()
                .join();
        assertThat(view.descriptor().batchCount()).isZero();
        assertThat(view.lookup(0)).isEmpty();
        assertThat(view.allowsPredecessorOffset(0)).isFalse();
        assertThat(view.gaps()).containsExactly(new KafkaCompactionRecordsV1.Gap(0, 1));
        assertThat(view.index(IndexKind.CHECKSUM_COVERAGE).rows()).hasSize(1);
    }

    @Test
    void wrongPhysicalIndexLocatorAndReadBudgetFailClosed() {
        var fixture = new Fixture(false);
        var original = fixture.descriptor;
        var locators = new ArrayList<>(original.indexes());
        var first = locators.get(0);
        locators.set(
                0,
                new KafkaSealedBookKeeperDescriptorV2.IndexLocator(
                        first.firstPart(),
                        first.firstEntry() + 1,
                        first.chunkCount(),
                        first.artifactLength(),
                        first.artifactSha256()));
        var bad = new KafkaSealedBookKeeperDescriptorV2(
                original.task(),
                original.sourceGeneration(),
                original.batchCount(),
                original.dispositionRootSha256(),
                original.gapRootSha256(),
                original.semanticProof(),
                original.sealedParts(),
                locators);
        assertThatThrownBy(() -> fixture.reader(1_000_000)
                        .recover(bad)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("recovered BK artifact count or physical index locators differ from descriptor");
        int reads = fixture.reads;
        int captures = fixture.captures;
        assertThatThrownBy(() -> fixture.reader(1).recover(original)).isInstanceOf(IllegalStateException.class);
        assertThat(fixture.reads).isEqualTo(reads);
        assertThat(fixture.captures).isEqualTo(captures);
    }

    @Test
    void corruptedPartPreventsTheWholeDescriptorFromBeingSelected() {
        var fixture = new Fixture(false);
        var seal = fixture.descriptor.sealedParts().get(0);
        var bodies = new ArrayList<>(
                fixture.entries.get(seal.handle().ledgerIdentity().ledgerId()));
        byte[] bad = bodies.get(0).toByteArray();
        bad[bad.length - 1] ^= 1;
        bodies.set(0, CanonicalBytes.copyOf(bad));
        fixture.entries.put(seal.handle().ledgerIdentity().ledgerId(), bodies);
        assertThatThrownBy(fixture::publish)
                .hasRootCauseMessage("selected BK complete part checksum or length differs");
        assertThat(fixture.store.selectorCasCount).isZero();
        assertThat(fixture.m4.readSelector())
                .contains(fixture.input.plan().sourceCut().predecessorSelector());
    }

    @Test
    void lostSelectorResponseReconcilesExactDescriptorAndRetryDoesNotSelectAgain() {
        var fixture = new Fixture(false);
        fixture.store.loseNextSelectorCasResponse = true;
        assertThat(fixture.publish()).isEqualTo(PublicationOutcome.EXISTING_EXACT);
        var selected = fixture.m4.readSelector().orElseThrow();
        assertThat(selected.selectedViewSha256()).isEqualTo(fixture.descriptor.descriptorSha256());
        assertThat(fixture.store.selectorCasCount).isEqualTo(1);
        assertThat(fixture.publish()).isEqualTo(PublicationOutcome.EXISTING_EXACT);
        assertThat(fixture.store.selectorCasCount).isEqualTo(1);
        var recovered = fixture.publication
                .recoverSelected(selected)
                .toCompletableFuture()
                .join();
        assertThat(recovered.lookup(0).orElseThrow().coverage().inclusiveStart())
                .isEqualTo(1);
    }

    @Test
    void unappliedImmutableOrSelectorResponseNeverClaimsSelectionAndRetriesExactCandidate() {
        var fixture = new Fixture(false);
        fixture.store.dropNextPut = true;
        assertThat(fixture.publish()).isEqualTo(PublicationOutcome.OUTCOME_UNKNOWN);
        assertThat(fixture.store.selectorCasCount).isZero();
        fixture.store.dropNextSelectorCas = true;
        assertThat(fixture.publish()).isEqualTo(PublicationOutcome.DEFINITIVELY_NOT_APPLIED);
        assertThat(fixture.m4.readSelector())
                .contains(fixture.input.plan().sourceCut().predecessorSelector());
        assertThat(fixture.publish()).isEqualTo(PublicationOutcome.APPLIED_EXACT);
        assertThat(fixture.store.selectorCasCount).isEqualTo(2);
    }

    @Test
    void changedProtocolStateAndMissingSelectedDescriptorNeverUseOldInputs() {
        var fixture = new Fixture(false);
        var expected = new KafkaCompactionPublicationFenceV1().expected(fixture.input.plan());
        var stale = new KafkaCompactionPublicationFenceV1.Snapshot(
                expected.compactionPlanRootSha256(),
                expected.protocolStateRootSha256(),
                expected.policyGeneration() + 1,
                expected.frontiers());
        assertThatThrownBy(() -> fixture.publication
                        .publish(
                                fixture.input.plan(),
                                fixture.input.semantic(),
                                fixture.descriptor,
                                fixture.sources,
                                () -> stale)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("M5-B policy/root/frontier fence is stale before publication");
        assertThat(fixture.store.selectorCasCount).isZero();
        assertThat(fixture.publish()).isEqualTo(PublicationOutcome.APPLIED_EXACT);
        var selected = fixture.m4.readSelector().orElseThrow();
        fixture.store.values.remove(
                KafkaBookKeeperCompactionPublicationV2.descriptorKey(selected.selectedViewSha256()));
        int reads = fixture.reads;
        assertThatThrownBy(() -> fixture.publication.recoverSelected(selected))
                .hasMessage("selected BK descriptor is missing");
        assertThat(fixture.reads).isEqualTo(reads);
    }

    @Test
    void ownerTakeoverBetweenTheLastRereadAndSelectorCasWinsOverStalePublication() {
        var fixture = new Fixture(false);
        var old = fixture.m4.readSelector().orElseThrow();
        var ownerView = KafkaBookKeeperCompactionTestSupportV2.digest("new-owner-view");
        fixture.store.beforeNextSelectorCas = () -> assertThat(fixture.m4.grantTakeover(old, ownerView, 2, 8))
                .isEqualTo(M4ReadControlCoordinatorV1.Outcome.APPLIED);
        assertThat(fixture.publish()).isEqualTo(PublicationOutcome.CONFLICT);
        var actual = fixture.m4.readSelector().orElseThrow();
        assertThat(actual.ownerEpoch()).isEqualTo(2);
        assertThat(actual.selectedViewSha256()).isEqualTo(ownerView);
        assertThat(fixture.store.selectorCasCount).isEqualTo(2);
    }

    @Test
    void changedNativeSealedFingerprintPreventsReadsAndSelection() {
        var fixture = new Fixture(false);
        var expected = fixture.descriptor.sealedParts().get(0);
        var changed = new BookKeeperDeleteTargetV1(
                expected.handle(),
                expected.sealedLastEntryId(),
                expected.sealedLength(),
                expected.ensembleSize(),
                expected.writeQuorumSize(),
                expected.ackQuorumSize(),
                expected.digestType(),
                expected.passwordCredentialIdentityVersion(),
                expected.passwordSha256(),
                expected.metadataFormatVersion(),
                expected.metadataCToken(),
                KafkaBookKeeperCompactionTestSupportV2.digest("changed-native-metadata"));
        fixture.seals.put(expected.handle().ledgerIdentity().ledgerId(), changed);
        assertThatThrownBy(fixture::publish)
                .hasRootCauseMessage("selected BK native sealed metadata is missing, changed or unknown");
        assertThat(fixture.reads).isZero();
        assertThat(fixture.store.selectorCasCount).isZero();
    }

    @Test
    void competingNativeSelectionReconcilesUnknownPublicationTickets() {
        var fixture = new Fixture(false);
        var selected = fixture.descriptor;
        var seals = new ArrayList<>(selected.sealedParts());
        var first = seals.get(0);
        seals.set(
                0,
                new BookKeeperDeleteTargetV1(
                        first.handle(),
                        first.sealedLastEntryId(),
                        first.sealedLength(),
                        first.ensembleSize(),
                        first.writeQuorumSize(),
                        first.ackQuorumSize(),
                        first.digestType(),
                        first.passwordCredentialIdentityVersion(),
                        first.passwordSha256(),
                        first.metadataFormatVersion(),
                        first.metadataCToken(),
                        KafkaBookKeeperCompactionTestSupportV2.digest("losing-native-metadata")));
        var losing = new KafkaSealedBookKeeperDescriptorV2(
                selected.task(),
                selected.sourceGeneration(),
                selected.batchCount(),
                selected.dispositionRootSha256(),
                selected.gapRootSha256(),
                selected.semanticProof(),
                seals,
                selected.indexes());
        assertThat(losing.descriptorSha256()).isNotEqualTo(selected.descriptorSha256());

        var sourceResource = SyntheticDeleteAuthorityFixturesV2.resource(801);
        Map<Sha256Digest, List<PhysicalResourceIdV2>> membership = fixture.input.plan().sourceCut().sources().stream()
                .collect(Collectors.toMap(source -> source.sourceIdentitySha256(), source -> List.of(sourceResource)));
        var targets = KafkaBookKeeperPublicationTicketsV2.targets(losing, membership);
        var ticketsStore = new TicketStore();
        targets.forEach(resource -> ticketsStore.put(
                resource.authorityKey(),
                M5TargetDeleteAuthorityCodecV1.encodeAuthority(
                        SyntheticDeleteAuthorityFixturesV2.phases(resource).get(0))));
        var tickets = new KafkaBookKeeperPublicationTicketsV2(
                new M5TargetDeleteMultiWriterGuardV2(new M5TargetDeleteAuthorityCoordinatorV1(ticketsStore)),
                ignored -> CompletableFuture.completedFuture(membership),
                Runnable::run);
        var decisions = new M5TaskSelectionCoordinatorV2(
                fixture.store, 7, fixture.input.plan().sourceCut().identity().binding());
        java.util.function.Supplier<Optional<M5TaskSelectionDecisionV2>> currentDecision =
                () -> decisions.readDecision(losing.task().taskIdSha256());
        assertThat(tickets.publish(
                                fixture.input.plan(),
                                losing,
                                () -> CompletableFuture.completedFuture(PublicationOutcome.OUTCOME_UNKNOWN),
                                currentDecision)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(PublicationOutcome.OUTCOME_UNKNOWN);
        targets.forEach(resource -> assertThat(ticketsStore.tickets(resource)).isEqualTo(1));

        assertThat(fixture.publish()).isEqualTo(PublicationOutcome.APPLIED_EXACT);
        assertThat(decisions
                        .readDecision(losing.task().taskIdSha256())
                        .orElseThrow()
                        .selectedOutput())
                .contains(selected.descriptorSha256());
        var retriedMutation = new AtomicInteger();
        assertThat(tickets.publish(
                                fixture.input.plan(),
                                losing,
                                () -> {
                                    retriedMutation.incrementAndGet();
                                    return CompletableFuture.completedFuture(PublicationOutcome.CONFLICT);
                                },
                                currentDecision)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(PublicationOutcome.CONFLICT);
        assertThat(retriedMutation).hasValue(0);
        targets.forEach(resource -> assertThat(ticketsStore.tickets(resource)).isZero());
    }

    private static final class TicketStore implements ExactMetadataTransactionStoreV1 {
        private final Map<String, VersionedValue> values = new HashMap<>();
        private long version;

        void put(String key, CanonicalBytes bytes) {
            values.put(
                    key,
                    VersionedValue.of(
                            key,
                            bytes,
                            new MetadataVersion(CanonicalBytes.copyOf(ByteBuffer.allocate(Long.BYTES)
                                    .putLong(++version)
                                    .array()))));
        }

        int tickets(PhysicalResourceIdV2 resource) {
            return M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                            values.get(resource.authorityKey()).canonicalStoredBytes())
                    .activeWriterTickets()
                    .size();
        }

        public CompletionStage<Optional<VersionedValue>> read(String key) {
            return CompletableFuture.completedFuture(Optional.ofNullable(values.get(key)));
        }

        public CompletionStage<MutationOutcome> compareAndSet(
                Optional<VersionedValue> expected, String key, CanonicalBytes bytes) {
            if (!Optional.ofNullable(values.get(key)).equals(expected)) {
                return CompletableFuture.completedFuture(MutationOutcome.DEFINITIVE_CONFLICT);
            }
            put(key, bytes);
            return CompletableFuture.completedFuture(MutationOutcome.APPLIED_EXACT);
        }

        public CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction ignored) {
            throw new AssertionError("publication ticket test has no multi-key transaction");
        }

        public boolean supportsAtomicMultiKeyTransactions() {
            return false;
        }
    }

    static final class Fixture {
        final KafkaBookKeeperCompactionTestSupportV2.Input input;
        final Store store = new Store();
        final Map<Long, List<CanonicalBytes>> entries = new LinkedHashMap<>();
        final Map<Long, BookKeeperDeleteTargetV1> seals = new LinkedHashMap<>();
        final BookKeeperCellSession session;
        final KafkaSealedBookKeeperDescriptorV2 descriptor;
        final KafkaBookKeeperCompactionPublicationV2 publication;
        final M4ReadControlCoordinatorV1 m4;
        final List<com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity>
                sources;
        long nextLedger;
        int reads;
        int captures;

        Fixture(boolean empty) {
            input = KafkaBookKeeperCompactionTestSupportV2.input(empty, 1);
            session = (BookKeeperCellSession) Proxy.newProxyInstance(
                    BookKeeperCellSession.class.getClassLoader(),
                    new Class<?>[] {BookKeeperCellSession.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "capabilitySnapshot" -> input.layout().task().capability();
                        case "reserveLedgerIdentity" ->
                            CompletableFuture.completedFuture(
                                    ProviderMutationResultV1.appliedExact(new BookKeeperLedgerIdentity(++nextLedger)));
                        case "openRunLedger" ->
                            CompletableFuture.completedFuture(
                                    RunLedgerOpenResultV1.openedExact((RunLedgerHandleV1) args[0]));
                        case "readExactEntry" -> {
                            reads++;
                            var handle = (RunLedgerHandleV1) args[0];
                            long entry = (long) args[1];
                            var bytes = entries.get(handle.ledgerIdentity().ledgerId())
                                    .get(Math.toIntExact(entry));
                            yield CompletableFuture.completedFuture(RunLedgerReadResultV1.foundExact(
                                    new ExactLedgerEntryV1(handle, entry, bytes, Sha256Digest.hash(bytes))));
                        }
                        default ->
                            throw new AssertionError(
                                    "descriptor recovery attempted native mutation: " + method.getName());
                    });
            var inventory = new KafkaBookKeeperInventoryV2(store);
            inventory.register(input.layout().task());
            List<VerifiedPart> verified = new ArrayList<>();
            for (int ordinal = 0; ordinal < input.layout().parts().size(); ordinal++) {
                var part = inventory
                        .reservePart(input.layout().task(), ordinal, session)
                        .toCompletableFuture()
                        .join()
                        .orElseThrow();
                var body = input.layout().parts().get(ordinal);
                var cap = input.layout().task().capability();
                var seal = new BookKeeperDeleteTargetV1(
                        part.handle(),
                        body.entries().size() - 1L,
                        body.plan().length(),
                        cap.ensembleSize(),
                        cap.writeQuorumSize(),
                        cap.ackQuorumSize(),
                        cap.digestType(),
                        cap.credentialIdentityVersion(),
                        KafkaBookKeeperCompactionTestSupportV2.digest("synthetic-password"),
                        3,
                        ordinal,
                        KafkaBookKeeperCompactionTestSupportV2.digest("synthetic-native-metadata-" + ordinal));
                verified.add(new VerifiedPart(part, seal));
                seals.put(part.resource().ledgerId(), seal);
                entries.put(part.resource().ledgerId(), body.entries());
            }
            descriptor =
                    KafkaSealedBookKeeperDescriptorV2.create(input.plan(), input.semantic(), input.layout(), verified);
            sources = KafkaBookKeeperCompactionTestSupportV2.installReadAuthority(store, input);
            m4 = new M4ReadControlCoordinatorV1(
                    store, 7, input.plan().sourceCut().identity().binding());
            publication = new KafkaBookKeeperCompactionPublicationV2(
                    store, 7, input.plan().sourceCut().identity().binding(), reader(1_000_000));
        }

        KafkaSealedBookKeeperReaderV2 reader(int bound) {
            return new KafkaSealedBookKeeperReaderV2(
                    session,
                    handle -> {
                        captures++;
                        return CompletableFuture.completedFuture(new CaptureResult(
                                CaptureOutcome.EXACT_TARGET,
                                Optional.of(seals.get(handle.ledgerIdentity().ledgerId()))));
                    },
                    bound);
        }

        PublicationOutcome publish() {
            return publication
                    .publish(
                            input.plan(),
                            input.semantic(),
                            descriptor,
                            sources,
                            () -> new KafkaCompactionPublicationFenceV1().expected(input.plan()))
                    .toCompletableFuture()
                    .join();
        }
    }
}
