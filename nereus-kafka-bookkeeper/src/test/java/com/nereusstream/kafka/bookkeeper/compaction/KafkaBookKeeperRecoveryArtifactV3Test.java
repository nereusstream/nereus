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
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeProtocolBatchAdapterV1;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryEnvelopeV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointComponentsV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCommittedProducerStateV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperDescriptorV2.IndexLocator;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaBookKeeperClosedHistoryRecoveryV1;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.api.kafka.KafkaBookKeeperOwnerAuthorityV1;
import com.nereusstream.storage.api.kafka.KafkaOwnerAdmissionV1;
import com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootStateV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.SemanticAspect;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationSourceCut;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceExtent;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class KafkaBookKeeperRecoveryArtifactV3Test {
    @Test
    void replacementComparisonBindsIndependentBodiesRecoveryIndexesAndOriginalMembership() {
        var state = KafkaBookKeeperCompactionTestSupportV2.recoveryState(false);
        var input = KafkaBookKeeperCompactionTestSupportV2.inputWithRecovery(state, 25);
        var fixture = new KafkaSealedBookKeeperDescriptorV2Test.Fixture(input, state);
        var recovered = fixture.reader(1_000_000)
                .recover(fixture.descriptor)
                .toCompletableFuture()
                .join();
        var original =
                comparisonTargets(fixture.descriptor, fixture.descriptor.task().namespace());
        var value = KafkaBookKeeperReplacementSemanticsV2.verify(
                input.plan(), input.semantic(), fixture.descriptor, state, recovered, original);
        assertThat(value.comparisons().stream().map(KafkaBookKeeperReplacementSemanticsV2.Comparison::aspect))
                .containsExactly(SemanticAspect.values());
        assertThat(value.publicationTargetsSha256()).isEqualTo(Sha256Digest.hash(original.encode()));
        assertThat(KafkaBookKeeperReplacementSemanticsV2.decode(value.encode())).isEqualTo(value);
        var fact = new AuthorityFactV1(
                KafkaBookKeeperReplacementSemanticsV2.key(fixture.descriptor.descriptorSha256()),
                new MetadataVersion(CanonicalBytes.copyOf(new byte[] {1})),
                Sha256Digest.hash(value.encode()));
        assertThat(value.transfers(fixture.descriptor, fact)).hasSize(SemanticAspect.values().length);
        var incomplete = KafkaBookKeeperPublicationTargetsV2.of(
                fixture.descriptor,
                original.resources().stream()
                        .filter(resource -> ((PhysicalResourceIdV2.BookKeeperLedger) resource).ledgerId() != 666_000)
                        .toList());
        assertThatThrownBy(() -> value.sources(fixture.descriptor, fact, incomplete.encode(), Optional.empty()))
                .hasMessageContaining("original publication membership");
        assertThatThrownBy(() -> value.transfers(
                        fixture.descriptor,
                        new AuthorityFactV1(
                                fact.key(),
                                fact.metadataVersion(),
                                KafkaBookKeeperCompactionTestSupportV2.digest("other"))))
                .hasMessageContaining("exact comparison bytes");
        byte[] corrupt = value.encode().toByteArray();
        corrupt[corrupt.length - 1] ^= 1;
        assertThatThrownBy(() -> KafkaBookKeeperReplacementSemanticsV2.decode(CanonicalBytes.copyOf(corrupt)))
                .hasMessageContaining("corrupt");
        assertThatThrownBy(() -> KafkaBookKeeperReplacementSemanticsV2.decode(
                        CanonicalBytes.copyOf(Arrays.copyOf(corrupt, corrupt.length + 1))))
                .hasMessageContaining("length");
    }

    @Test
    void unchangedCheckpointProvenanceCannotHideChangedStateOrForeignPhysicalNamespace() {
        var state = KafkaBookKeeperCompactionTestSupportV2.recoveryState(false);
        var input = KafkaBookKeeperCompactionTestSupportV2.inputWithRecovery(state, 26);
        var fixture = new KafkaSealedBookKeeperDescriptorV2Test.Fixture(input, state);
        var recovered = fixture.reader(1_000_000)
                .recover(fixture.descriptor)
                .toCompletableFuture()
                .join();
        var original =
                comparisonTargets(fixture.descriptor, fixture.descriptor.task().namespace());
        var changed = new KafkaCompactionRecoveryStateV1(
                state.sourceFence(),
                state.coveredThrough(),
                state.sourceCheckpointSha256(),
                new KafkaProtocolCheckpointComponentsV1(
                        KafkaCommittedProducerStateV1.empty(),
                        state.components().transactionState(),
                        state.components().leaderEpochIndex()));
        assertThatThrownBy(() -> KafkaBookKeeperReplacementSemanticsV2.verify(
                        input.plan(), input.semantic(), fixture.descriptor, changed, recovered, original))
                .hasMessageContaining("RECOVERY");
        var namespace = fixture.descriptor.task().namespace();
        var foreign = new PhysicalResourceIdV2.Namespace(
                namespace.providerKind(),
                namespace.serviceIdentity(),
                com.nereusstream.domain.bytes.CanonicalUtf8.fromString("another-instance"));
        assertThatThrownBy(() -> KafkaBookKeeperReplacementSemanticsV2.verify(
                        input.plan(),
                        input.semantic(),
                        fixture.descriptor,
                        state,
                        recovered,
                        comparisonTargets(fixture.descriptor, foreign)))
                .hasMessageContaining("NATIVE_SOURCE_AUTHORITY");
        var outputOnly = KafkaBookKeeperPublicationTargetsV2.of(
                fixture.descriptor,
                original.resources().stream()
                        .filter(resource -> ((PhysicalResourceIdV2.BookKeeperLedger) resource).ledgerId() != 666_000)
                        .toList());
        assertThatThrownBy(() -> KafkaBookKeeperReplacementSemanticsV2.verify(
                        input.plan(), input.semantic(), fixture.descriptor, state, recovered, outputOnly))
                .hasMessageContaining("original physical input");
    }

    private static KafkaBookKeeperPublicationTargetsV2 comparisonTargets(
            KafkaSealedBookKeeperDescriptorV2 descriptor, PhysicalResourceIdV2.Namespace inputNamespace) {
        var resources = new ArrayList<PhysicalResourceIdV2>();
        resources.add(new PhysicalResourceIdV2.BookKeeperLedger(inputNamespace, 666_000));
        descriptor
                .sealedParts()
                .forEach(seal -> resources.add(new PhysicalResourceIdV2.BookKeeperLedger(
                        descriptor.task().namespace(),
                        seal.handle().ledgerIdentity().ledgerId())));
        return KafkaBookKeeperPublicationTargetsV2.of(
                descriptor, resources.stream().sorted().distinct().toList());
    }

    @Test
    void versionedDescriptorAndChunksKeepProtocolStateSeparateFromEightSemanticIndexes() {
        var state = KafkaBookKeeperCompactionTestSupportV2.recoveryState(true);
        var input = KafkaBookKeeperCompactionTestSupportV2.inputWithRecovery(state, 20);
        var fixture = new KafkaSealedBookKeeperDescriptorV2Test.Fixture(input, state);
        var descriptor = fixture.descriptor;
        assertThat(state.encode().length()).isGreaterThan(KafkaSealedBookKeeperDescriptorV2.MAX_ENCODED_BYTES);
        assertThat(descriptor.encode().length()).isLessThan(KafkaSealedBookKeeperDescriptorV2.MAX_ENCODED_BYTES);
        assertThat(descriptor.wireVersion()).isEqualTo(3);
        assertThat(KafkaSealedBookKeeperDescriptorCodecV2.decode(descriptor.encode()))
                .isEqualTo(descriptor);
        var locator = descriptor.recoveryStateLocator().orElseThrow();
        assertThat(locator.chunkCount()).isGreaterThan(1);
        assertThat(descriptor.sealedParts().size()).isGreaterThan(locator.firstPart() + 1);
        var view = fixture.reader(Math.toIntExact(input.layout().task().parts().stream()
                        .mapToLong(KafkaBookKeeperInventoryV2.PartPlan::length)
                        .sum()))
                .recover(descriptor)
                .toCompletableFuture()
                .join();
        assertThat(view.recoveryState()).contains(state);
        assertThat(descriptor.indexes()).hasSize(8);
        var legacy = new KafkaSealedBookKeeperDescriptorV2Test.Fixture(false).descriptor;
        assertThat(legacy.wireVersion()).isEqualTo(2);
        assertThat(legacy.recoveryStateLocator()).isEmpty();
        for (int length : List.of(
                0, 5, descriptor.encode().length() - 1, descriptor.encode().length() + 1)) {
            assertThatThrownBy(() -> KafkaSealedBookKeeperDescriptorCodecV2.decode(CanonicalBytes.copyOf(
                            Arrays.copyOf(descriptor.encode().toByteArray(), length))))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        byte[] wrong = descriptor.encode().toByteArray();
        wrong[5] = 2;
        assertThatThrownBy(() -> KafkaSealedBookKeeperDescriptorCodecV2.decode(CanonicalBytes.copyOf(wrong)))
                .isInstanceOf(IllegalArgumentException.class);
        var entry = input.layout().parts().get(locator.firstPart()).entries().get(locator.firstEntry());
        byte[] wrongChunk = entry.toByteArray();
        wrongChunk[5] = 2;
        assertThatThrownBy(() -> KafkaBookKeeperCompactionLayoutV2.decodeChunk(
                        CanonicalBytes.copyOf(wrongChunk),
                        input.layout().task().capability().maximumAddPayloadBytes()))
                .hasMessageContaining("version or kind");
    }

    @Test
    void missingWrongAndDuplicatedRecoveryArtifactsCannotBeIgnoredByASelectedView() {
        var state = KafkaBookKeeperCompactionTestSupportV2.recoveryState(false);
        var input = KafkaBookKeeperCompactionTestSupportV2.inputWithRecovery(state, 21);
        var fixture = new KafkaSealedBookKeeperDescriptorV2Test.Fixture(input, state);
        var descriptor = fixture.descriptor;
        var locator = descriptor.recoveryStateLocator().orElseThrow();
        for (var recovery : List.of(
                Optional.<IndexLocator>empty(),
                Optional.of(new IndexLocator(
                        locator.firstPart(),
                        locator.firstEntry(),
                        locator.chunkCount(),
                        locator.artifactLength(),
                        KafkaBookKeeperCompactionTestSupportV2.digest("wrong-recovery"))))) {
            var wrong = new KafkaSealedBookKeeperDescriptorV2(
                    descriptor.task(),
                    descriptor.sourceGeneration(),
                    descriptor.batchCount(),
                    descriptor.dispositionRootSha256(),
                    descriptor.gapRootSha256(),
                    descriptor.semanticProof(),
                    descriptor.sealedParts(),
                    descriptor.indexes(),
                    recovery);
            assertThatThrownBy(() -> fixture.reader(1_000_000)
                            .recover(wrong)
                            .toCompletableFuture()
                            .join())
                    .hasRootCauseMessage(
                            "recovered BK artifact count or physical index locators differ from descriptor");
        }
        var parts = new ArrayList<>(input.layout().parts());
        parts.add(parts.get(parts.size() - 1));
        var original = descriptor.task();
        var task = new KafkaBookKeeperInventoryV2.Task(
                original.sourceCut(),
                original.compactionPlanRootSha256(),
                original.semanticOutputSha256(),
                original.semanticValidationRootSha256(),
                original.namespace(),
                original.capability(),
                original.attempt(),
                parts.stream()
                        .map(KafkaBookKeeperCompactionLayoutV2.PartBody::plan)
                        .toList());
        assertThatThrownBy(() -> KafkaBookKeeperArtifactAssemblerV2.assemble(task, parts))
                .hasMessageContaining("missing, duplicate or reordered");
    }

    @Test
    void claimedCheckpointDigestCannotSubstituteForTheFullCapturedRecoveryStateAtPublication() {
        var state = KafkaBookKeeperCompactionTestSupportV2.recoveryState(false);
        var input = KafkaBookKeeperCompactionTestSupportV2.inputWithRecovery(state, 22);
        var fixture = new KafkaSealedBookKeeperDescriptorV2Test.Fixture(input, state);
        var changed = new KafkaCompactionRecoveryStateV1(
                state.sourceFence(),
                state.coveredThrough(),
                state.sourceCheckpointSha256(),
                new KafkaProtocolCheckpointComponentsV1(
                        KafkaCommittedProducerStateV1.empty(),
                        state.components().transactionState(),
                        state.components().leaderEpochIndex()));
        assertThatThrownBy(() -> fixture.publication.publish(
                        input.plan(),
                        input.semantic(),
                        fixture.descriptor,
                        fixture.sources,
                        () -> new KafkaCompactionPublicationFenceV1().expected(input.plan())))
                .hasMessageContaining("requires its captured");
        assertThatThrownBy(() -> fixture.publication.publish(
                        input.plan(),
                        input.semantic(),
                        fixture.descriptor,
                        fixture.sources,
                        () -> new KafkaCompactionPublicationFenceV1().expected(input.plan()),
                        changed))
                .hasMessageContaining("differs from its captured");
        var locator = fixture.descriptor.recoveryStateLocator().orElseThrow();
        var seal = fixture.descriptor.sealedParts().get(locator.firstPart());
        var entries = new ArrayList<>(
                fixture.entries.get(seal.handle().ledgerIdentity().ledgerId()));
        byte[] corrupt = entries.get(locator.firstEntry()).toByteArray();
        corrupt[corrupt.length - 1] ^= 1;
        entries.set(locator.firstEntry(), CanonicalBytes.copyOf(corrupt));
        fixture.entries.put(seal.handle().ledgerIdentity().ledgerId(), entries);
        assertThatThrownBy(fixture::publish)
                .hasRootCauseMessage("selected BK complete part checksum or length differs");
        assertThat(fixture.store.selectorCasCount).isZero();
    }

    @Test
    void outputRecoveryStateMustEndAtTheExactSourceCutAndKeepItsCheckpointRoot() {
        var state = KafkaBookKeeperCompactionTestSupportV2.recoveryState(false);
        var input = KafkaBookKeeperCompactionTestSupportV2.inputWithRecovery(state, 23);
        for (var wrong : List.of(
                new KafkaCompactionRecoveryStateV1(
                        state.sourceFence(), 3, state.sourceCheckpointSha256(), state.components()),
                new KafkaCompactionRecoveryStateV1(
                        state.sourceFence(),
                        state.coveredThrough(),
                        KafkaBookKeeperCompactionTestSupportV2.digest("foreign"),
                        state.components()))) {
            assertThatThrownBy(() -> KafkaBookKeeperCompactionLayoutV2.plan(
                            input.plan(),
                            input.semantic(),
                            wrong,
                            input.layout().task().namespace(),
                            input.layout().task().capability(),
                            24,
                            64 * 1024,
                            256 * 1024))
                    .hasMessageContaining("differs from its source cut");
        }
    }

    @Test
    void retiredRawPrefixKeepsItsClosedLineageAndIndependentSelectedProtocolState() {
        var history = new SelectedHistory();
        var recovered = history.recover(history.prefix.retire(), true, false);
        assertThat(recovered.endOffset()).isEqualTo(2);
        assertThat(recovered.closedOwners()).containsExactly(history.first, history.second);
        assertThat(recovered.admittedRoots()).isEmpty();
        assertThat(recovered.runs()).isEmpty();
        assertThat(recovered.readRuns()).isEmpty();
        assertThat(recovered.selectedPrefixAnchor()).contains(history.prefix.root());
        assertThat(recovered.lastRoot()).contains(history.prefix.root());
        assertThat(recovered.selectedCheckpointEndOffset()).hasValue(2);
        assertThat(recovered.progress().entries()).isZero();
        var restored = recovered.protocolState().orElseThrow();
        assertThat(restored.components()).isEqualTo(history.state.components());
        assertThat(restored.readIndexes()).isEmpty();
        assertThat(restored.vector().runBinding().runId())
                .isEqualTo(
                        history.view.descriptor().sealedParts().get(0).handle().runId());
        assertThat(restored.vector().runBinding().runId())
                .isNotEqualTo(history.prefix.root().runId());
    }

    @Test
    void selectedRecoveryRejectsChangedPhysicalMembersBoundaryAndOwnerClosureBeforeSdkWork() {
        var history = new SelectedHistory();
        var mismatchedSource = new SelectedHistory(42);
        assertThatThrownBy(() -> mismatchedSource.recover(mismatchedSource.prefix, true, false))
                .hasRootCauseMessage("selected recovery source differs from its closed raw prefix member");
        var foreign = new KafkaRunRootRecordV2(
                new PhysicalResourceIdV2.BookKeeperLedger(
                        history.prefix.resource().namespace(), 42),
                history.root(42, 2),
                true,
                Optional.empty());
        assertThatThrownBy(() -> history.recover(foreign, true, false))
                .hasRootCauseMessage("selected recovery source differs from its closed raw prefix member");
        var crossing = new KafkaRunRootRecordV2(history.prefix.resource(), history.root(41, 3), true, Optional.empty());
        assertThatThrownBy(() -> history.recover(crossing, true, false))
                .hasRootCauseMessage("selected recovery cut crosses a raw run boundary");
        assertThatThrownBy(() -> history.recover(history.prefix, false, false))
                .hasRootCauseMessage("closed owner selected a run missing from its birth inventory");
        assertThatThrownBy(() -> history.recover(history.prefix, true, true))
                .hasRootCauseMessage("closed owner predecessor identity or digest differs");
    }

    @Test
    void maximumCoveredRunCatalogDoesNotGrowTheRecoveryCallbackStack() {
        var history = new SelectedHistory();
        var records = new ArrayList<KafkaRunRootRecordV2>();
        records.add(history.prefix.retire());
        var source = history.prefix.root();
        for (int index = 1; index < KafkaBookKeeperClosedHistoryRecoveryV1.MAX_HISTORY_RUNS; index++) {
            var previous = records.get(index - 1);
            var root = new KafkaRunRootSnapshotV1(
                    source.bindingId(),
                    source.topicIncarnation(),
                    source.partitionId(),
                    source.storageEpochId(),
                    1,
                    1,
                    source.providerScopeId(),
                    new StorageRunId(new Id128(0, 5_000 + index)),
                    new BookKeeperLedgerIdentity(1_000 + index),
                    2,
                    OptionalLong.of(2),
                    KafkaRunRootStateV1.SEALED,
                    Optional.of(previous.root().runId()));
            var record = new KafkaRunRootRecordV2(
                    new PhysicalResourceIdV2.BookKeeperLedger(
                            history.prefix.resource().namespace(), 1_000 + index),
                    root,
                    true,
                    Optional.empty());
            records.set(
                    index - 1,
                    new KafkaRunRootRecordV2(
                            previous.resource(), previous.root(), true, Optional.of(record.initialLink()), true));
            records.add(record.retire());
        }
        var recovered = history.recover(records, true, false);
        assertThat(recovered.selectedPrefixAnchor())
                .contains(records.get(records.size() - 1).root());
        assertThat(recovered.protocolState().orElseThrow().components()).isEqualTo(history.state.components());
        assertThat(recovered.admittedRoots()).isEmpty();
        assertThat(recovered.progress().entries()).isZero();
    }

    /** Synthetic carrier and metadata unit fixture; it supplies no native selection or deletion qualification. */
    private static final class SelectedHistory {
        private final KafkaCompactionRecoveryStateV1 state =
                KafkaBookKeeperCompactionTestSupportV2.recoveryState(false);
        private final KafkaBookKeeperReadViewV2 view;
        private final KafkaRunRootRecordV2 prefix;
        private final KafkaOwnerAdmissionV1 first;
        private final KafkaOwnerAdmissionV1 second;

        private SelectedHistory() {
            this(41);
        }

        private SelectedHistory(long sourceLedgerId) {
            var original = KafkaBookKeeperCompactionTestSupportV2.inputWithRecovery(state, 27);
            prefix = new KafkaRunRootRecordV2(
                    new PhysicalResourceIdV2.BookKeeperLedger(
                            original.layout().task().namespace(), 41),
                    root(original, 41, 2),
                    true,
                    Optional.empty());
            var previous = original.plan().sourceCut();
            var source = previous.sources().get(0);
            var extent = new SourceExtent(
                    source.kind(),
                    source.sourceIdentitySha256(),
                    source.coverage(),
                    source.physicalKey(),
                    source.canonicalLength(),
                    source.recordCount(),
                    source.minimumTimestamp(),
                    source.maximumTimestamp(),
                    source.bodySha256(),
                    source.immutableProviderVersionToken(),
                    Optional.of(new PhysicalResourceIdV2.BookKeeperLedger(
                                    prefix.resource().namespace(), sourceLedgerId)
                            .sha256()),
                    source.formatRootSha256(),
                    source.encryptionPolicySha256(),
                    source.payloadLongLivedReadable(),
                    source.requiredIndexesPresent(),
                    source.memberBindingIds());
            var cut = new MaterializationSourceCut(
                    previous.identity(),
                    previous.predecessorSelector(),
                    previous.predecessorSelectorValueSha256(),
                    previous.predecessorViewSha256(),
                    previous.coverage(),
                    previous.durableFrontier(),
                    previous.logEndFrontier(),
                    previous.highWatermark(),
                    previous.lastStableFrontier(),
                    previous.trimFrontier(),
                    previous.protocolStateRootSha256(),
                    previous.recoveryCheckpointRootSha256(),
                    previous.materializationPolicySha256(),
                    previous.outputFormatPolicySha256(),
                    M5MaterializationCodecV1.calculateSourceSetSha256(List.of(extent)),
                    List.of(extent));
            var plan = original.plan();
            var rebound = new KafkaCompactionRecordsV1.CompactionPlan(
                    cut,
                    plan.policy(),
                    plan.frontiers(),
                    plan.protocolRoots(),
                    plan.inputBatches(),
                    plan.keyProofs(),
                    plan.transactions(),
                    plan.leaderEpochs(),
                    plan.undecidableOffsets(),
                    plan.recoveryRequiredOffsets());
            var semantic = new KafkaSemanticCompactorV1().compileSemantic(rebound);
            var layout = KafkaBookKeeperCompactionLayoutV2.plan(
                    rebound,
                    semantic,
                    state,
                    original.layout().task().namespace(),
                    original.layout().task().capability(),
                    27,
                    64 * 1024,
                    256 * 1024);
            var fixture = new KafkaSealedBookKeeperDescriptorV2Test.Fixture(
                    new KafkaBookKeeperCompactionTestSupportV2.Input(
                            rebound, semantic, layout, original.capabilityEvidence()),
                    state);
            view = fixture.reader(1_000_000)
                    .recover(fixture.descriptor)
                    .toCompletableFuture()
                    .join();
            first = new KafkaOwnerAdmissionV1(
                    Sha256Digest.hash(
                            KafkaRunRootRecordV2.Scope.of(prefix.root()).encode()),
                    new KafkaOwnerIdentityV1(1, 1, 1, 1, 1),
                    true,
                    Optional.empty(),
                    List.of(prefix.initialLink()));
            second = second(false);
        }

        private KafkaRunRootSnapshotV1 root(long ledgerId, long end) {
            var binding = view.descriptor().task().capability();
            return root(binding.providerScopeId(), ledgerId, end);
        }

        private KafkaRunRootSnapshotV1 root(
                KafkaBookKeeperCompactionTestSupportV2.Input input, long ledgerId, long end) {
            return root(input.layout().task().capability().providerScopeId(), ledgerId, end);
        }

        private KafkaRunRootSnapshotV1 root(
                com.nereusstream.storage.api.bookkeeper.CellProviderScopeId provider, long ledgerId, long end) {
            var fence = state.sourceFence();
            return new KafkaRunRootSnapshotV1(
                    fence.bindingId(),
                    fence.topicIncarnation(),
                    fence.partitionId(),
                    fence.storageEpochId(),
                    1,
                    1,
                    provider,
                    new StorageRunId(new Id128(0, 6)),
                    new BookKeeperLedgerIdentity(ledgerId),
                    0,
                    OptionalLong.of(end),
                    KafkaRunRootStateV1.SEALED,
                    Optional.empty());
        }

        private KafkaOwnerAdmissionV1 second(boolean corrupt) {
            return second(first, corrupt);
        }

        private KafkaOwnerAdmissionV1 second(KafkaOwnerAdmissionV1 predecessor, boolean corrupt) {
            return new KafkaOwnerAdmissionV1(
                    first.scopeSha256(),
                    new KafkaOwnerIdentityV1(2, 2, 2, 2, 2),
                    true,
                    Optional.of(new KafkaOwnerAdmissionV1.Previous(
                            1,
                            corrupt
                                    ? KafkaBookKeeperCompactionTestSupportV2.digest("wrong-owner-closure")
                                    : Sha256Digest.hash(predecessor.encode()),
                            predecessor.tail())),
                    List.of());
        }

        private KafkaBookKeeperClosedHistoryRecoveryV1.Result recover(
                KafkaRunRootRecordV2 record, boolean birthPresent, boolean corruptClosure) {
            return recover(List.of(record), birthPresent, corruptClosure);
        }

        private KafkaBookKeeperClosedHistoryRecoveryV1.Result recover(
                List<KafkaRunRootRecordV2> records, boolean birthPresent, boolean corruptClosure) {
            var predecessor = new KafkaOwnerAdmissionV1(
                    first.scopeSha256(),
                    first.owner(),
                    true,
                    Optional.empty(),
                    records.stream().map(KafkaRunRootRecordV2::initialLink).toList());
            var selected = second(predecessor, corruptClosure);
            var authority = (KafkaBookKeeperOwnerAuthorityV1) Proxy.newProxyInstance(
                    KafkaBookKeeperOwnerAuthorityV1.class.getClassLoader(),
                    new Class<?>[] {KafkaBookKeeperOwnerAuthorityV1.class},
                    (proxy, method, arguments) -> switch (method.getName()) {
                        case "readClosedOwner" ->
                            CompletableFuture.completedFuture(
                                    Optional.of((long) arguments[0] == 1 ? predecessor : selected));
                        case "readRunBirths" ->
                            CompletableFuture.completedFuture(Optional.of(
                                    birthPresent
                                                    && ((KafkaOwnerAdmissionV1) arguments[0])
                                                                    .owner()
                                                                    .ownerEpoch()
                                                            == 1
                                            ? records
                                            : List.of()));
                        case "readAdmittedRun" ->
                            CompletableFuture.completedFuture(records.stream()
                                    .filter(record -> record.initialLink().equals(arguments[0]))
                                    .findFirst());
                        default -> throw new AssertionError("unexpected metadata operation: " + method.getName());
                    });
            var session = (BookKeeperCellSession) Proxy.newProxyInstance(
                    BookKeeperCellSession.class.getClassLoader(),
                    new Class<?>[] {BookKeeperCellSession.class},
                    (proxy, method, arguments) -> switch (method.getName()) {
                        case "providerScopeId" ->
                            view.descriptor().task().capability().providerScopeId();
                        case "capabilitySnapshot" -> view.descriptor().task().capability();
                        default ->
                            throw new AssertionError("covered prefix must not dispatch SDK work: " + method.getName());
                    });
            var source = state.sourceFence();
            var fence = new KafkaPartitionFenceV1(
                    source.bindingId(),
                    source.topicIncarnation(),
                    source.partitionId(),
                    1,
                    source.storageEpochId(),
                    3,
                    3);
            return new KafkaBookKeeperClosedHistoryRecoveryV1(
                            session,
                            authority,
                            new KafkaNativeProtocolBatchAdapterV1(),
                            System::nanoTime,
                            (closed, birth, handle) -> {
                                throw new AssertionError("selected prefix must not acquire an unselected birth fence");
                            })
                    .recoverSelected(
                            selected,
                            fence,
                            0,
                            new KafkaBookKeeperRecoveryEnvelopeV1(100, 1_000_000, 30_000_000_000L),
                            view)
                    .toCompletableFuture()
                    .join();
        }
    }
}
