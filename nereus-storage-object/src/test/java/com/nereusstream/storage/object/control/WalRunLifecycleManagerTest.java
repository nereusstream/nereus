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

package com.nereusstream.storage.object.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.ArrayList;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class WalRunLifecycleManagerTest {
    @Test
    void responseLossAcceptsOnlyExactImmutableCandidate() {
        TestControlMetadataStore store = new TestControlMetadataStore();
        WalRunLifecycleManager manager = manager(store);
        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        store.nextMode(TestControlMetadataStore.NextMode.APPLY_BUT_UNKNOWN);

        WalRunReference reference = manager.createRoot(WalRunControlKeys.rootKey(7, 1), root);

        assertThat(reference.rootSha256()).isEqualTo(WalRunControlCodec.rootSha256(root));
        store.nextMode(TestControlMetadataStore.NextMode.UNKNOWN_WITHOUT_APPLY);
        WalRunRootRecord secondRoot = ObjectWalControlTestFixtures.root(2, Optional.empty());
        assertThatThrownBy(() -> manager.createRoot(WalRunControlKeys.rootKey(7, 2), secondRoot))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("exact candidate");
    }

    @Test
    void successorIsCreatedBeforeExactPointerCas() {
        TestControlMetadataStore store = new TestControlMetadataStore();
        WalRunLifecycleManager manager = manager(store);
        WalRunRootRecord predecessorRoot = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalRunReference predecessor = manager.createRoot(WalRunControlKeys.rootKey(7, 1), predecessorRoot);
        CurrentWalRunPointer pointer = new CurrentWalRunPointer(predecessor);
        manager.initializePointer(WalRunControlKeys.pointerKey(7), pointer);
        WalCheckpointHeadV1 checkpointHead =
                WalCheckpointHeadV1.empty(predecessor.rootSha256(), predecessor.shardRunEpoch(), 1);
        com.nereusstream.domain.bytes.CanonicalBytes checkpointHeadBytes =
                WalRunControlCodec.encodeCheckpointHead(checkpointHead);
        store.putExact(WalRunControlKeys.checkpointHeadKey(7, 1), checkpointHeadBytes);
        WalRunSealRecord seal = new WalRunSealRecord(
                predecessor,
                LaneSequenceVector.empty(),
                WalRunControlKeys.checkpointHeadKey(7, 1),
                com.nereusstream.domain.bytes.Sha256Digest.hash(checkpointHeadBytes),
                0,
                0);
        assertThatThrownBy(() ->
                        manager.publishSeal(WalRunControlKeys.sealKey(7, 1), seal, new WalRunRuntime(predecessorRoot)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("stopped/sealed runtime closure");
        WalRunTerminalClosureProofV1 closure =
                manager.publishSeal(WalRunControlKeys.sealKey(7, 1), seal, sealedRuntime(predecessorRoot));
        assertThat(closure.root()).isEqualTo(predecessor);
        assertThatThrownBy(() -> ObjectWalControlTestFixtures.root(
                        2,
                        Optional.of(new WalRunPredecessor(
                                predecessor,
                                WalRunControlKeys.sealKey(7, 1),
                                WalRunControlCodec.sealSha256(seal),
                                Optional.empty()))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires the exact terminal Kafka");
        WalRunRootRecord successorRoot = ObjectWalControlTestFixtures.root(
                2,
                Optional.of(new WalRunPredecessor(
                        predecessor,
                        WalRunControlKeys.sealKey(7, 1),
                        WalRunControlCodec.sealSha256(seal),
                        Optional.of(terminalBinding(1)))));
        WalRunRootRecord nonterminalSuccessorRoot = ObjectWalControlTestFixtures.root(
                2,
                Optional.of(new WalRunPredecessor(
                        predecessor,
                        WalRunControlKeys.sealKey(7, 1),
                        WalRunControlCodec.sealSha256(seal),
                        Optional.of(terminalBinding(1, false)))));
        WalRunRootRecord substitutedHeadShaRoot = ObjectWalControlTestFixtures.root(
                2,
                Optional.of(new WalRunPredecessor(
                        predecessor,
                        WalRunControlKeys.sealKey(7, 1),
                        WalRunControlCodec.sealSha256(seal),
                        Optional.of(new TerminalProtocolCheckpointBindingV1(
                                com.nereusstream.domain.protocol.ProtocolKindV1.KAFKA,
                                terminalHeadKey(1),
                                ObjectWalControlTestFixtures.digest(99))))));

        assertThatThrownBy(() -> manager.publishSuccessorAndAdvance(
                        WalRunControlKeys.pointerKey(7),
                        pointer,
                        WalRunControlKeys.sealKey(7, 1),
                        seal,
                        WalRunControlKeys.rootKey(7, 2),
                        successorRoot))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Head is absent");

        store.putExact(terminalHeadKey(1), terminalHeadBytes(1, false));
        assertThatThrownBy(() -> manager.publishSuccessorAndAdvance(
                        WalRunControlKeys.pointerKey(7),
                        pointer,
                        WalRunControlKeys.sealKey(7, 1),
                        seal,
                        WalRunControlKeys.rootKey(7, 2),
                        nonterminalSuccessorRoot))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not terminal");

        store.putExact(terminalHeadKey(1), terminalHeadBytes(1, true));
        assertThatThrownBy(() -> manager.publishSuccessorAndAdvance(
                        WalRunControlKeys.pointerKey(7),
                        pointer,
                        WalRunControlKeys.sealKey(7, 1),
                        seal,
                        WalRunControlKeys.rootKey(7, 2),
                        substitutedHeadShaRoot))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SHA-256 differs");

        WalRunReference successor = manager.publishSuccessorAndAdvance(
                WalRunControlKeys.pointerKey(7),
                pointer,
                WalRunControlKeys.sealKey(7, 1),
                seal,
                WalRunControlKeys.rootKey(7, 2),
                successorRoot);

        assertThat(manager.readPointer(WalRunControlKeys.pointerKey(7)).current())
                .isEqualTo(successor);
        assertThat(store.operations().indexOf("create:" + WalRunControlKeys.rootKey(7, 2)))
                .isLessThan(store.operations().lastIndexOf("cas:" + WalRunControlKeys.pointerKey(7)));
    }

    @Test
    void absentExactSealFailsClosedBeforeSuccessorPublication() {
        TestControlMetadataStore store = new TestControlMetadataStore();
        WalRunLifecycleManager manager = manager(store);
        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalRunReference reference = ObjectWalControlTestFixtures.reference(WalRunControlKeys.rootKey(7, 1), root);
        WalRunSealRecord seal = new WalRunSealRecord(
                reference,
                LaneSequenceVector.empty(),
                WalRunControlKeys.checkpointHeadKey(7, 1),
                ObjectWalControlTestFixtures.digest(4),
                0,
                0);

        assertThatThrownBy(() -> manager.publishSuccessorAndAdvance(
                        WalRunControlKeys.pointerKey(7),
                        new CurrentWalRunPointer(reference),
                        WalRunControlKeys.sealKey(7, 1),
                        seal,
                        WalRunControlKeys.rootKey(7, 2),
                        ObjectWalControlTestFixtures.root(2, Optional.empty())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Seal is absent");
    }

    @Test
    void sealAggregateMustEqualCompleteVerifiedPageChain() {
        TestControlMetadataStore store = new TestControlMetadataStore();
        WalRunLifecycleManager manager = manager(store);
        WalRunRootRecord root = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalRunReference reference = manager.createRoot(WalRunControlKeys.rootKey(7, 1), root);
        ProviderResolvedExtentRowV1 row = new ProviderResolvedExtentRowV1(
                WalLaneId.OBJECT_LATENCY,
                0,
                256,
                512,
                ObjectWalControlTestFixtures.digest(8),
                ProviderVersionProof.none());
        WalRunCheckpointPageV1 page = new WalRunCheckpointPageV1(
                reference.rootSha256(), 0, Optional.empty(), java.util.List.of(row), LaneSequenceVector.of(0, -1, -1));
        com.nereusstream.domain.bytes.CanonicalBytes pageBytes = WalRunControlCodec.encodeCheckpointPage(page);
        com.nereusstream.domain.bytes.Sha256Digest pageSha = com.nereusstream.domain.bytes.Sha256Digest.hash(pageBytes);
        String pageKey = WalRunControlKeys.checkpointPageKey(7, 1, 0, pageSha);
        store.putExact(pageKey, pageBytes);
        WalCheckpointHeadV1 head = new WalCheckpointHeadV1(
                reference.rootSha256(),
                1,
                1,
                0,
                Optional.of(pageKey),
                Optional.of(pageSha),
                LaneSequenceVector.of(0, -1, -1));
        com.nereusstream.domain.bytes.CanonicalBytes headBytes = WalRunControlCodec.encodeCheckpointHead(head);
        store.putExact(WalRunControlKeys.checkpointHeadKey(7, 1), headBytes);
        WalRunSealRecord wrong = new WalRunSealRecord(
                reference,
                head.coveredThrough(),
                WalRunControlKeys.checkpointHeadKey(7, 1),
                com.nereusstream.domain.bytes.Sha256Digest.hash(headBytes),
                2,
                512);

        assertThatThrownBy(() -> manager.publishSeal(WalRunControlKeys.sealKey(7, 1), wrong, sealedRuntime(root)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("aggregate facts");
    }

    @Test
    void sealedPointerCrashNeedsCandidateThenAdvancesOrAdoptsOnlyExactLineage() {
        TestControlMetadataStore store = new TestControlMetadataStore();
        WalRunLifecycleManager manager = manager(store);
        WalRunRootRecord predecessorRoot = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalRunReference predecessor = manager.createRoot(WalRunControlKeys.rootKey(7, 1), predecessorRoot);
        manager.initializePointer(WalRunControlKeys.pointerKey(7), new CurrentWalRunPointer(predecessor));
        WalCheckpointHeadV1 head = WalCheckpointHeadV1.empty(predecessor.rootSha256(), 1, 1);
        com.nereusstream.domain.bytes.CanonicalBytes headBytes = WalRunControlCodec.encodeCheckpointHead(head);
        store.putExact(WalRunControlKeys.checkpointHeadKey(7, 1), headBytes);
        WalRunSealRecord seal = new WalRunSealRecord(
                predecessor,
                LaneSequenceVector.empty(),
                WalRunControlKeys.checkpointHeadKey(7, 1),
                com.nereusstream.domain.bytes.Sha256Digest.hash(headBytes),
                0,
                0);
        manager.publishSeal(WalRunControlKeys.sealKey(7, 1), seal, sealedRuntime(predecessorRoot));
        store.putExact(terminalHeadKey(1), terminalHeadBytes(1, true));
        WalRunRootRecord successorRoot = ObjectWalControlTestFixtures.root(
                2,
                Optional.of(new WalRunPredecessor(
                        predecessor,
                        WalRunControlKeys.sealKey(7, 1),
                        WalRunControlCodec.sealSha256(seal),
                        Optional.of(terminalBinding(1)))));
        WalRunLifecycleManager.SuccessorCandidate candidate =
                new WalRunLifecycleManager.SuccessorCandidate(WalRunControlKeys.rootKey(7, 2), successorRoot);
        WalRunLifecycleManager.SuccessorCandidate crossCell = new WalRunLifecycleManager.SuccessorCandidate(
                WalRunControlKeys.rootKey(7, 2),
                copyWithProtocolCell(
                        successorRoot,
                        new com.nereusstream.domain.protocol.KafkaProtocolCellIdentity(
                                new com.nereusstream.domain.identity.DeploymentId(
                                        new com.nereusstream.domain.identity.Id128(9, 10)),
                                new com.nereusstream.domain.identity.KafkaCellId(
                                        new com.nereusstream.domain.identity.Id128(11, 12)))));
        assertThatThrownBy(() -> copyWithProtocolCell(
                        successorRoot,
                        new com.nereusstream.domain.protocol.PulsarProtocolCellIdentity(
                                new com.nereusstream.domain.identity.DeploymentId(
                                        new com.nereusstream.domain.identity.Id128(20, 21)),
                                new com.nereusstream.domain.identity.ReservationDomainId(
                                        new com.nereusstream.domain.identity.Id128(22, 23)),
                                new com.nereusstream.domain.identity.PulsarCellId(
                                        new com.nereusstream.domain.identity.Id128(24, 25)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Pulsar M3 successor Root must omit");

        assertThat(manager.completeSealedPointer(WalRunControlKeys.pointerKey(7), predecessor, Optional.empty())
                        .outcome())
                .isEqualTo(WalRunLifecycleManager.SealedPointerCompletionOutcome.NEED_EXACT_SUCCESSOR_CANDIDATE);

        assertThatThrownBy(() -> manager.completeSealedPointer(
                        WalRunControlKeys.pointerKey(7), predecessor, Optional.of(crossCell)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("substituted the Protocol Cell");

        manager.createRoot(candidate.rootKey(), candidate.root());
        store.nextMode(TestControlMetadataStore.NextMode.APPLY_BUT_UNKNOWN);
        WalRunLifecycleManager.SealedPointerCompletion advanced =
                manager.completeSealedPointer(WalRunControlKeys.pointerKey(7), predecessor, Optional.of(candidate));
        assertThat(advanced.outcome()).isEqualTo(WalRunLifecycleManager.SealedPointerCompletionOutcome.ADVANCED_EXACT);
        assertThat(manager.completeSealedPointer(WalRunControlKeys.pointerKey(7), predecessor, Optional.empty())
                        .outcome())
                .isEqualTo(WalRunLifecycleManager.SealedPointerCompletionOutcome.ALREADY_ADVANCED_EXACT);

        WalRunRootRecord fork = ObjectWalControlTestFixtures.root(3, Optional.empty());
        WalRunReference forkReference = manager.createRoot(WalRunControlKeys.rootKey(7, 3), fork);
        store.putExact(
                WalRunControlKeys.pointerKey(7),
                WalRunControlCodec.encodePointer(new CurrentWalRunPointer(forkReference)));
        assertThatThrownBy(() ->
                        manager.completeSealedPointer(WalRunControlKeys.pointerKey(7), predecessor, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("predecessor");
    }

    @Test
    void differentCasWinnerRequiresOwnerOpenUnderWinnerEnvelopeWithoutThirdRootRead() {
        TestControlMetadataStore store = new TestControlMetadataStore();
        WalRunLifecycleManager manager = manager(store);
        WalRunRootRecord predecessorRoot = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalRunReference predecessor = manager.createRoot(WalRunControlKeys.rootKey(7, 1), predecessorRoot);
        CurrentWalRunPointer expectedPointer = new CurrentWalRunPointer(predecessor);
        manager.initializePointer(WalRunControlKeys.pointerKey(7), expectedPointer);
        WalCheckpointHeadV1 head = WalCheckpointHeadV1.empty(predecessor.rootSha256(), 1, 1);
        com.nereusstream.domain.bytes.CanonicalBytes headBytes = WalRunControlCodec.encodeCheckpointHead(head);
        store.putExact(WalRunControlKeys.checkpointHeadKey(7, 1), headBytes);
        WalRunSealRecord seal = new WalRunSealRecord(
                predecessor,
                LaneSequenceVector.empty(),
                WalRunControlKeys.checkpointHeadKey(7, 1),
                com.nereusstream.domain.bytes.Sha256Digest.hash(headBytes),
                0,
                0);
        manager.publishSeal(WalRunControlKeys.sealKey(7, 1), seal, sealedRuntime(predecessorRoot));
        store.putExact(terminalHeadKey(1), terminalHeadBytes(1, true));
        WalRunPredecessor lineage = new WalRunPredecessor(
                predecessor,
                WalRunControlKeys.sealKey(7, 1),
                WalRunControlCodec.sealSha256(seal),
                Optional.of(terminalBinding(1)));
        WalRunRootRecord candidateRoot = ObjectWalControlTestFixtures.root(2, Optional.of(lineage));
        WalRunRootRecord winnerRoot = ObjectWalControlTestFixtures.root(3, Optional.of(lineage));
        WalRunReference winner = manager.createRoot(WalRunControlKeys.rootKey(7, 3), winnerRoot);
        store.putExact(
                WalRunControlKeys.pointerKey(7), WalRunControlCodec.encodePointer(new CurrentWalRunPointer(winner)));

        assertThatThrownBy(() -> manager.publishSuccessorAndAdvance(
                        WalRunControlKeys.pointerKey(7),
                        expectedPointer,
                        WalRunControlKeys.sealKey(7, 1),
                        seal,
                        WalRunControlKeys.rootKey(7, 2),
                        candidateRoot))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("retry owner-open");
        assertThat(store.operations().stream()
                        .filter(("get:" + winner.rootKey())::equals)
                        .count())
                .isZero();

        WalRunLifecycleManager.SealedPointerCompletion recovered =
                manager.completeSealedPointer(WalRunControlKeys.pointerKey(7), predecessor, Optional.empty());
        assertThat(recovered.outcome())
                .isEqualTo(WalRunLifecycleManager.SealedPointerCompletionOutcome.ALREADY_ADVANCED_EXACT);
        assertThat(recovered.current()).isEqualTo(winner);
    }

    @Test
    void successorRetainedLineageReadsEachRootOnceAndRoutesTerminalReadersByExactRetainedRoot() {
        TestControlMetadataStore store = new TestControlMetadataStore();
        WalRunRootRecord oldestRoot = ObjectWalControlTestFixtures.root(1, Optional.empty());
        WalRunReference oldest = ObjectWalControlTestFixtures.reference(WalRunControlKeys.rootKey(7, 1), oldestRoot);
        WalCheckpointHeadV1 oldestCheckpointHead =
                WalCheckpointHeadV1.empty(oldest.rootSha256(), oldest.shardRunEpoch(), 1);
        com.nereusstream.domain.bytes.CanonicalBytes oldestCheckpointHeadBytes =
                WalRunControlCodec.encodeCheckpointHead(oldestCheckpointHead);
        WalRunSealRecord oldestSeal = new WalRunSealRecord(
                oldest,
                LaneSequenceVector.empty(),
                WalRunControlKeys.checkpointHeadKey(7, 1),
                com.nereusstream.domain.bytes.Sha256Digest.hash(oldestCheckpointHeadBytes),
                0,
                0);
        store.putExact(oldest.rootKey(), WalRunControlCodec.encodeRoot(oldestRoot));
        store.putExact(WalRunControlKeys.checkpointHeadKey(7, 1), oldestCheckpointHeadBytes);
        store.putExact(WalRunControlKeys.sealKey(7, 1), WalRunControlCodec.encodeSeal(oldestSeal));
        store.putExact(terminalHeadKey(1), terminalHeadBytes(1, true));

        WalRunRootRecord directRoot = ObjectWalControlTestFixtures.root(
                2,
                Optional.of(new WalRunPredecessor(
                        oldest,
                        WalRunControlKeys.sealKey(7, 1),
                        WalRunControlCodec.sealSha256(oldestSeal),
                        Optional.of(terminalBinding(1)))));
        WalRunReference direct = ObjectWalControlTestFixtures.reference(WalRunControlKeys.rootKey(7, 2), directRoot);
        WalCheckpointHeadV1 directCheckpointHead =
                WalCheckpointHeadV1.empty(direct.rootSha256(), direct.shardRunEpoch(), 1);
        com.nereusstream.domain.bytes.CanonicalBytes directCheckpointHeadBytes =
                WalRunControlCodec.encodeCheckpointHead(directCheckpointHead);
        WalRunSealRecord directSeal = new WalRunSealRecord(
                direct,
                LaneSequenceVector.empty(),
                WalRunControlKeys.checkpointHeadKey(7, 2),
                com.nereusstream.domain.bytes.Sha256Digest.hash(directCheckpointHeadBytes),
                0,
                0);
        store.putExact(direct.rootKey(), WalRunControlCodec.encodeRoot(directRoot));
        store.putExact(WalRunControlKeys.checkpointHeadKey(7, 2), directCheckpointHeadBytes);
        store.putExact(WalRunControlKeys.sealKey(7, 2), WalRunControlCodec.encodeSeal(directSeal));
        store.putExact(terminalHeadKey(2), terminalHeadBytes(2, true));

        WalRunRootRecord successorRoot = ObjectWalControlTestFixtures.root(
                3,
                Optional.of(new WalRunPredecessor(
                        direct,
                        WalRunControlKeys.sealKey(7, 2),
                        WalRunControlCodec.sealSha256(directSeal),
                        Optional.of(terminalBinding(2)))));
        WalRunReference expectedSuccessor =
                ObjectWalControlTestFixtures.reference(WalRunControlKeys.rootKey(7, 3), successorRoot);
        CurrentWalRunPointer expectedPointer = new CurrentWalRunPointer(direct);
        store.putExact(WalRunControlKeys.pointerKey(7), WalRunControlCodec.encodePointer(expectedPointer));

        com.nereusstream.domain.bytes.CanonicalBytes selectedObjectBytes =
                com.nereusstream.domain.bytes.CanonicalBytes.copyOf(new byte[] {42});
        com.nereusstream.storage.object.provider.ObjectIdentity selectedObject =
                new com.nereusstream.storage.object.provider.ObjectIdentity(
                        "terminal-selected",
                        selectedObjectBytes.length(),
                        com.nereusstream.domain.bytes.Sha256Digest.hash(selectedObjectBytes));
        ArrayList<WalRunRootRecord> readerRoots = new ArrayList<>();
        WalRunLifecycleManager manager = new WalRunLifecycleManager(
                store,
                (root, seal, binding, exactValue, context) -> {
                    assertThat(binding.protocolKind()).isEqualTo(com.nereusstream.domain.protocol.ProtocolKindV1.KAFKA);
                    assertThat(exactValue).isEqualTo(terminalHeadBytes(root.shardRunEpoch(), true));
                    assertThat(context.budgetOwnerRoot()).isEqualTo(successorRoot);
                    assertThat(context.protocolObjectRoot()).isEqualTo(root);
                    assertThat(context.readVerifiedProtocolObject(selectedObject))
                            .isEqualTo(selectedObjectBytes);
                },
                protocolObjectRoot -> {
                    readerRoots.add(protocolObjectRoot);
                    if (!protocolObjectRoot.equals(directRoot) && !protocolObjectRoot.equals(oldestRoot)) {
                        throw new IllegalStateException("terminal reader factory received a substituted retained Root");
                    }
                    return identity -> {
                        assertThat(identity).isEqualTo(selectedObject);
                        return selectedObjectBytes;
                    };
                });

        WalRunReference successor = manager.publishSuccessorAndAdvance(
                WalRunControlKeys.pointerKey(7),
                expectedPointer,
                WalRunControlKeys.sealKey(7, 2),
                directSeal,
                WalRunControlKeys.rootKey(7, 3),
                successorRoot);

        assertThat(successor).isEqualTo(expectedSuccessor);
        assertThat(store.operations().stream()
                        .filter(("get:" + direct.rootKey())::equals)
                        .count())
                .isEqualTo(1);
        assertThat(store.operations().stream()
                        .filter(("get:" + oldest.rootKey())::equals)
                        .count())
                .isEqualTo(1);
        assertThat(readerRoots).containsExactly(directRoot, oldestRoot);
    }

    @Test
    void versionProofWireRoundTripsButProductionRootPublicationRejectsIt() {
        WalRunRootRecord versionRoot = withProofMode(
                ObjectWalControlTestFixtures.root(1, Optional.empty()),
                ProviderProofMode.VERSION_BOUND_FULL_OBJECT_SHA256_V1,
                32);
        assertThat(WalRunControlCodec.decodeRoot(WalRunControlCodec.encodeRoot(versionRoot)))
                .isEqualTo(versionRoot);
        assertThatThrownBy(() -> new WalRunLifecycleManager(new TestControlMetadataStore())
                        .createRoot(WalRunControlKeys.rootKey(7, 1), versionRoot))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ProviderProofMode.NONE");
    }

    private static WalRunRootRecord copyWithProtocolCell(
            WalRunRootRecord root, com.nereusstream.domain.protocol.ProtocolCellIdentity protocolCellIdentity) {
        return new WalRunRootRecord(
                root.shardId(),
                root.shardRunEpoch(),
                root.walRunSessionId(),
                root.openedAtMillis(),
                protocolCellIdentity,
                root.providerScopeId(),
                root.formatContract(),
                root.nwg1AdmissionCaps(),
                root.bounds(),
                root.checkpointPolicy(),
                root.providerConfiguration(),
                root.recoveryEnvelope(),
                root.wrappedRunKey(),
                root.predecessor());
    }

    private static WalRunRootRecord withProofMode(WalRunRootRecord root, ProviderProofMode proofMode, int tokenCap) {
        ObjectProviderRootConfiguration provider = root.providerConfiguration();
        return new WalRunRootRecord(
                root.shardId(),
                root.shardRunEpoch(),
                root.walRunSessionId(),
                root.openedAtMillis(),
                root.protocolCellIdentity(),
                root.providerScopeId(),
                root.formatContract(),
                root.nwg1AdmissionCaps(),
                root.bounds(),
                root.checkpointPolicy(),
                new ObjectProviderRootConfiguration(
                        provider.accessProfile(),
                        provider.adapterVersion(),
                        provider.canonicalizerVersion(),
                        provider.exclusiveNamespacePrefix(),
                        proofMode,
                        tokenCap,
                        provider.maxObjectBodyBytes(),
                        provider.maxSinglePutBytes(),
                        provider.maxSingleRangeReadBytes(),
                        provider.maxPrefixSegmentsPerExtent(),
                        provider.maxListPageKeys(),
                        provider.capabilityReceiptSha256()),
                root.recoveryEnvelope(),
                root.wrappedRunKey(),
                root.predecessor());
    }

    private static WalRunLifecycleManager manager(TestControlMetadataStore store) {
        return new WalRunLifecycleManager(store, (root, seal, binding, exactValue, context) -> {
            byte[] bytes = exactValue.toByteArray();
            if (binding.protocolKind() != com.nereusstream.domain.protocol.ProtocolKindV1.KAFKA
                    || bytes.length != 2
                    || bytes[0] != 1
                    || bytes[1] != (byte) root.shardRunEpoch()) {
                throw new IllegalStateException("test Kafka protocol Head is not terminal/exact for the predecessor");
            }
            assertThat(context.protocolObjectRoot()).isEqualTo(root);
        });
    }

    private static WalRunRuntime sealedRuntime(WalRunRootRecord root) {
        WalRunRuntime runtime = new WalRunRuntime(root);
        runtime.stopAdmission(WalRunRuntime.StopReason.OWNER_REQUEST);
        runtime.seal();
        return runtime;
    }

    private static String terminalHeadKey(long epoch) {
        return "v2/object-wal/protocol/kafka/run-" + epoch + "/head";
    }

    private static com.nereusstream.domain.bytes.CanonicalBytes terminalHeadBytes(long epoch, boolean terminal) {
        return com.nereusstream.domain.bytes.CanonicalBytes.copyOf(
                new byte[] {(byte) (terminal ? 1 : 0), (byte) epoch});
    }

    private static TerminalProtocolCheckpointBindingV1 terminalBinding(long epoch) {
        return terminalBinding(epoch, true);
    }

    private static TerminalProtocolCheckpointBindingV1 terminalBinding(long epoch, boolean terminal) {
        return new TerminalProtocolCheckpointBindingV1(
                com.nereusstream.domain.protocol.ProtocolKindV1.KAFKA,
                terminalHeadKey(epoch),
                com.nereusstream.domain.bytes.Sha256Digest.hash(terminalHeadBytes(epoch, terminal)));
    }
}
